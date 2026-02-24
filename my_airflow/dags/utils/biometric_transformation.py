from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, to_date, current_timestamp, split
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.window import Window
import logging
import sys
import os
import re
import io
import pandas as pd

# Add utils path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.shared_transformation import SharedTransformationJob

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def biometric_transformation():
    """
    Medallion Architecture: Bronze → Silver → Gold → MySQL
    -------------------------------------------------------
    Bronze : Raw Excel data converted to Delta (biometric_raw)
    Silver : Cleaned, typed, numeric minutes computed (biometric_cleaned)
    Gold   : Dimension-enriched fact table (fact_attendance_biometric)
    MySQL  : Incremental upsert into fact_attendance_biometric
    NOTE   : Biometric files are per-date Excel sheets, processed file by file.
             Each file's records are appended to Bronze/Silver/Gold.
    """
    job = SharedTransformationJob(app_name="biometric_transformation_bulk_file")

    BLOB_PREFIX    = "biometric/raw/"
    PASSED_PREFIX  = "biometric/passed/"
    UC_BRONZE = "hr_analytics.bronze.biometric_raw"
    UC_SILVER = "hr_analytics.silver.biometric_cleaned"
    UC_GOLD   = "hr_analytics.gold.fact_attendance_biometric"
    MYSQL_TABLE    = "fact_attendance_biometric"

    # ---------------------------------------------------------------
    # Helper: sanitise Excel column names → valid Spark column names
    # ---------------------------------------------------------------
    def clean_col(c: str) -> str:
        c = re.sub(r"[.\s]+", "_", c.strip())
        return re.sub(r"_+", "_", c).strip("_")

    # ---------------------------------------------------------------
    # Helper: convert "HH:MM" string column → integer minutes
    # ---------------------------------------------------------------
    def time_to_minutes(col_name):
        return (
            split(col(col_name), ":")[0].cast("int") * 60 +
            split(col(col_name), ":")[1].cast("int")
        )

    # ---------------------------------------------------------------
    # Helper: safe Excel reader (handles both .xls and .xlsx)
    # ---------------------------------------------------------------
    def read_excel_safe(bytes_io, sheet_name=None, skiprows=None, nrows=None, header=0, is_xls=False):
        bytes_io.seek(0)
        try:
            return pd.read_excel(bytes_io, sheet_name=sheet_name, skiprows=skiprows, nrows=nrows, header=header)
        except Exception as e:
            if is_xls:
                bytes_io.seek(0)
                return pd.read_excel(bytes_io, sheet_name=sheet_name, skiprows=skiprows, nrows=nrows, header=header, engine="xlrd")
            raise e

    try:
        # ============================================================
        # STEP 1: List all biometric Excel files from Azure Blob
        # ============================================================
        blob_list = [
            b.name for b in job.container_client.list_blobs(name_starts_with=BLOB_PREFIX)
            if b.name.lower().endswith((".xls", ".xlsx"))
        ]
        logger.info(f"📄 Found {len(blob_list)} biometric Excel files to process.")

        if not blob_list:
            logger.info("No biometric files found. Exiting.")
            job.stop()
            return

        # ============================================================
        # STEP 1: Process Each File -> Ingest into Bronze
        # ============================================================
        processed_dates = set()
        for blob_name in blob_list:
            logger.info(f"🚀 Ingesting: {blob_name}")
            temp_json = None
            try:
                # Download and read metadata for date extraction
                blob_data = job.container_client.download_blob(blob_name).readall()
                xls_bytes = io.BytesIO(blob_data)
                is_xls = blob_name.lower().endswith(".xls")

                metadata_df = read_excel_safe(xls_bytes, nrows=9, header=None, is_xls=is_xls)
                if isinstance(metadata_df, dict):
                    metadata_df = pd.DataFrame(list(metadata_df.values())[0])

                metadata_text = "\n".join(metadata_df.fillna("").astype(str).stack().tolist())
                m = re.search(r"Attendance Date\s*[:\-]\s*(\d{2}-\w{3}-\d{4})", metadata_text)
                work_date_str = m.group(1) if m else None
                
                if not work_date_str:
                    logger.warning(f"   Could not find 'Attendance Date' in {blob_name}. Skipping.")
                    continue
                
                processed_dates.add(work_date_str)

                # Read all sheets (skip 9 rows)
                xls_bytes.seek(0)
                engine_arg = "xlrd" if is_xls else None
                excel = pd.ExcelFile(xls_bytes, engine=engine_arg)
                sheets = []
                for sheet in excel.sheet_names:
                    xls_bytes.seek(0)
                    df_sheet = read_excel_safe(xls_bytes, sheet_name=sheet, skiprows=skiprows if 'skiprows' in locals() else 9, is_xls=is_xls)
                    df_sheet["SourceSheet"] = sheet
                    sheets.append(df_sheet)

                if sheets:
                    df_all = pd.concat(sheets, ignore_index=True)
                    temp_json = f"/tmp/temp_biometric_{os.path.basename(blob_name)}.json"
                    df_all.to_json(temp_json, orient="records", lines=True)

                    df_spark = job.spark.read.json(temp_json)
                    df_spark = df_spark.toDF(*[clean_col(c) for c in df_spark.columns])
                    
                    # Add business date to Bronze for easier filtering later
                    df_spark = df_spark.withColumn("ingest_work_date", to_date(lit(work_date_str), "dd-MMM-yyyy"))
                    
                    job.write_to_bronze(df_spark, "biometric_raw")
                    logger.info(f"   ✅ Ingested {df_spark.count()} rows into Bronze for {work_date_str}")

                # Archive immediately after successful Bronze write
                src_blob = job.container_client.get_blob_client(blob_name)
                dest_name = blob_name.replace(BLOB_PREFIX, PASSED_PREFIX)
                dest_blob = job.container_client.get_blob_client(dest_name)
                dest_blob.start_copy_from_url(src_blob.url)
                src_blob.delete_blob()

                if temp_json and os.path.exists(temp_json): os.remove(temp_json)

            except Exception as e:
                logger.error(f"❌ Failed to ingest {blob_name}: {e}")
                if temp_json and os.path.exists(temp_json): os.remove(temp_json)

        if not processed_dates:
            logger.info("No valid biometric data ingested. Job stop.")
            job.stop()
            return

        # ============================================================
        # STEP 2 — SILVER: vectorized transformation + deduplication
        # ============================================================
        logger.info("=== SILVER: Starting vectorized transformation ===")
        df_bronze_all = job.read_from_uc(UC_BRONZE)
        logger.info(f"   Total rows in Bronze: {df_bronze_all.count()}")
        
        # Filter Bronze to only the dates we just ingested to keep it fast
        # Use a more robust date comparison
        df_bronze = df_bronze_all.filter(
            F.date_format(col("ingest_work_date"), "dd-MMM-yyyy").isin(list(processed_dates)) |
            col("ingest_work_date").isNull() # Catch-all for first-time runs
        )
        
        # If we still have 0 records with the strict filter, fallback to a count check
        bronze_count = df_bronze.count()
        logger.info(f"   Rows after date-filtering: {bronze_count}")
        
        if bronze_count == 0 and processed_dates:
            logger.warning("   ⚠️ No records found after filtering. Checking schema...")
            df_bronze_all.printSchema()

        df_clean = df_bronze.select(
            # Handle potential case-sensitivity or missing columns gracefully
            F.coalesce(
                *[col(c) for c in df_bronze.columns if c.lower() in ["e_code", "ecode"]]
            ).cast(IntegerType()).alias("employee_id"),
            F.coalesce(
                *[col(c) for c in df_bronze.columns if c.lower() in ["name", "empname"]]
            ).alias("name_in_bio"),
            col("Shift").alias("shift_in_bio"),
            col("A_InTime").alias("bio_in_time_str"),
            col("A_OutTime").alias("bio_out_time_str"),
            col("Work_Dur").alias("bio_work_dur_str"),
            col("OT").alias("ot_str"),
            col("Tot_Dur").alias("bio_total_dur_str"),
            col("Status").alias("bio_status"),
            col("ingest_work_date").alias("work_date"),
            col("ingest_timestamp")
        ).filter(col("employee_id").isNotNull())

        df_clean = df_clean \
            .withColumn("work_minutes",  time_to_minutes("bio_work_dur_str")) \
            .withColumn("total_minutes", time_to_minutes("bio_total_dur_str")) \
            .withColumn("ot_minutes",    time_to_minutes("ot_str"))

        # Deduplication: One record per employee per day (Keep latest ingest)
        window_spec = Window.partitionBy("employee_id", "work_date").orderBy(col("ingest_timestamp").desc())
        df_silver = df_clean.withColumn("rn", F.row_number().over(window_spec)).filter("rn = 1").drop("rn")

        silver_count = df_silver.count()
        logger.info(f"✅ Silver transformation complete: {silver_count} records.")
        job.write_to_silver(df_silver, "biometric_cleaned", mode="append")

        # ============================================================
        # STEP 3 — GOLD: Dimension Enrichment
        # ============================================================
        logger.info("=== GOLD: Enriching with dimension keys ===")
        df_enriched = job.enrich_with_dimension_keys(df_silver, employee_col="employee_id", date_col="work_date")

        df_gold = df_enriched.select(
            "dim_employee_key", "dim_date_key",
            df_silver["employee_id"].cast("string").alias("source_bio_id"),
            "name_in_bio", "shift_in_bio", "bio_in_time_str", "bio_out_time_str",
            "bio_work_dur_str", "ot_str", "bio_total_dur_str", "bio_status",
            "work_date", "work_minutes", "total_minutes", "ot_minutes",
            current_timestamp().alias("dw_load_timestamp")
        ).filter(col("dim_employee_key").isNotNull())

        gold_count = df_gold.count()
        logger.info(f"✅ Gold records ready: {gold_count}")
        job.write_to_gold(df_gold, "fact_attendance_biometric", mode="append")

        # ============================================================
        # STEP 4 — MYSQL: Incremental Sync (Delete-Insert by Date)
        # ============================================================
        logger.info("=== MYSQL: Syncing to database ===")
        import mysql.connector
        conn = mysql.connector.connect(
            host=job.mysql_host, port=int(job.mysql_port), user=job.mysql_user,
            password=job.mysql_password, database=job.mysql_db, auth_plugin="mysql_native_password"
        )
        cursor = conn.cursor()
        
        # Vectorized delete for all dates in batch
        placeholders = ",".join(["STR_TO_DATE(%s, '%d-%b-%Y')"] * len(processed_dates))
        cursor.execute(f"DELETE FROM {MYSQL_TABLE} WHERE work_date IN ({placeholders})", list(processed_dates))
        conn.commit()
        cursor.close(); conn.close()

        job.write_to_mysql(df_gold, MYSQL_TABLE, mode="append")
        logger.info(f"✅ Biometric transformation complete for {len(processed_dates)} dates.")

        logger.info("✅ Biometric transformation complete (all files processed).")

    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise e
    finally:
        job.stop()


if __name__ == "__main__":
    biometric_transformation()
