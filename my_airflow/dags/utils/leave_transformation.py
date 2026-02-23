from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, struct, lit, date_format, current_timestamp
)
from pyspark.sql.window import Window
import logging
import sys
import os

# Add utils path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.shared_transformation import SharedTransformationJob

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_leave_transform():
    """
    Medallion Architecture: Bronze → Silver → Gold → MySQL
    -------------------------------------------------------
    Bronze : Raw JSONL from Azure Blob (leave_raw)
    Silver : Cleaned, typed, flattened (leave_cleaned)
    Gold   : Dimension-enriched fact table (fact_leave)
    MySQL  : Incremental upsert into fact_leave
    """
    job = SharedTransformationJob(app_name="ZohoLeaveProcessor")

    SOURCE_PATH  = "Zoho_leave/raw/zoho_people_leave/"
    SUCCESS_PATH = "Zoho_leave/success_files/zoho_people_leave/"
    UC_BRONZE = "hr_analytics.bronze.leave_raw"
    UC_SILVER = "hr_analytics.silver.leave_cleaned"
    UC_GOLD   = "hr_analytics.gold.fact_leave"
    MYSQL_TABLE  = "fact_leave"

    try:
        # ============================================================
        # STEP 1: Read source files from Azure Blob Storage
        # ============================================================
        file_list = job.list_files(SOURCE_PATH, ".jsonl")
        if not file_list:
            logger.info("No files to process. Exiting.")
            job.stop()
            return

        df_raw = job.read_files(file_list, download_locally=True, file_type="text")
        if df_raw is None:
            logger.warning("read_files returned None. Exiting.")
            job.stop()
            return

        logger.info(f"Total raw records read: {df_raw.count()}")

        # ============================================================
        # STEP 2 — BRONZE: Store raw JSON lines as-is in Delta Lake
        # ============================================================
        logger.info("=== BRONZE: Writing raw leave data (as text) ===")
        # Rename column to 'raw_json' for clarity in the Bronze layer
        job.write_to_bronze(df_raw.select(col("value").alias("raw_json")), "leave_raw")

        # ============================================================
        # STEP 3 — SILVER: Flatten, clean, type-cast
        # ============================================================
        logger.info("=== SILVER: Reading Bronze and transforming ===")
        df_bronze = job.read_from_uc(UC_BRONZE)

        # ---- Dynamic Flattening of Zoho leave nested structure ----
        # Preserve ingest_timestamp for deduplication
        # Use coalesce to handle varied nesting (response.result or flat map)
        df_flat_raw = df_bronze.select(
            col("ingest_timestamp"),
            F.explode(
                F.from_json(
                    F.coalesce(
                        F.get_json_object(col("raw_json"), "$._airbyte_data.response.result"),
                        F.get_json_object(col("raw_json"), "$.response.result"),
                        F.get_json_object(col("raw_json"), "$.result"),
                        F.get_json_object(col("raw_json"), "$._airbyte_data")
                    ),
                    "MAP<STRING, STRING>"
                )
            ).alias("zoho_record_id", "record_json")
        ).filter(~col("zoho_record_id").isin("status", "message", "total_count"))
        
        # Define a schema for parsing the record JSON
        record_schema = """
            STRUCT<
                EmployeeId: STRING,
                `Zoho.ID`: LONG,
                ApprovalStatus: STRING,
                DateOfRequest: STRING,
                `From`: STRING,
                `To`: STRING,
                Leavetype: STRING,
                TeamEmailID: STRING,
                Unit: STRING,
                ZUID: LONG,
                Reason: STRING,
                Days: MAP<STRING, STRUCT<StartTime:STRING, EndTime:STRING, LeaveCount:STRING, Session:STRING>>
            >
        """
        
        df_flat = df_flat_raw.withColumn("record", F.from_json(col("record_json"), record_schema)) \
                             .select("ingest_timestamp", "zoho_record_id", "record.*") \
                             .filter(col("EmployeeId").isNotNull())

        # ---- Handle the dynamic 'Days' map to extract leave details ----
        df_cleaned_raw = df_flat.select(
            col("*"),
            F.map_values(col("Days"))[0].alias("first_day")
        ).select(
            col("*"),
            col("first_day.StartTime").alias("StartTime"),
            col("first_day.EndTime").alias("EndTime"),
            col("first_day.LeaveCount").alias("LeaveCount"),
            col("first_day.Session").alias("Session")
        ).drop("Days", "first_day")

        # ---- Type-cast and rename all columns ----
        date_formats = ["dd-MMM-yyyy", "yyyy-MM-dd"]
        time_formats = ["yyyy-MM-dd HH:mm:ss", "HH:mm:ss", "dd-MMM-yyyy HH:mm:ss"]
        leave_count_col = "LeaveCount" if "LeaveCount" in df_cleaned_raw.columns else "Leave_count"

        df_clean = df_cleaned_raw.select(
            col("ingest_timestamp"),
            col("EmployeeId").alias("employee_id"),
            col("`Zoho.ID`").cast("long").alias("source_leave_id"),
            col("zoho_record_id"),
            col("ApprovalStatus").alias("approval_status"),
            F.coalesce(*[F.expr(f'to_date(trim(DateOfRequest), "{f}")') for f in date_formats]).alias("date_of_request"),
            F.coalesce(*[F.expr(f'to_date(trim(`From`), "{f}")') for f in date_formats]).alias("from_date"),
            F.coalesce(*[F.expr(f'to_date(trim(`To`), "{f}")') for f in date_formats]).alias("to_date"),
            col("Leavetype").alias("leave_type"),
            col("TeamEmailID").alias("team_email_id"),
            col("Unit").alias("leave_unit"),
            col("ZUID").cast("long").alias("zuid_in_leave"),
            col("reason"),
            date_format(
                F.coalesce(*[F.expr(f'to_timestamp(trim(StartTime), "{f}")') for f in time_formats]),
                "HH:mm:ss"
            ).alias("leave_start_time"),
            date_format(
                F.coalesce(*[F.expr(f'to_timestamp(trim(EndTime), "{f}")') for f in time_formats]),
                "HH:mm:ss"
            ).alias("leave_end_time"),
            col(leave_count_col).cast("decimal(4,1)").alias("leave_count")
        ).filter(col("employee_id").isNotNull() & col("from_date").isNotNull())

        # ---- Deduplication: Keep only the latest ingest per source_leave_id ----
        window_spec = Window.partitionBy("source_leave_id").orderBy(col("ingest_timestamp").desc())
        df_silver = df_clean \
            .withColumn("row_num", F.row_number().over(window_spec)) \
            .filter(col("row_num") == 1) \
            .drop("row_num", "ingest_timestamp")

        logger.info(f"Silver records after dedup: {df_silver.count()}")
        job.write_to_silver(df_silver, "leave_cleaned")

        # ============================================================
        # STEP 4 — GOLD: Enrich with dimension keys, build fact table
        # ============================================================
        logger.info("=== GOLD: Reading Silver and enriching with dim keys ===")
        df_silver_read = job.read_from_uc(UC_SILVER)
        df_enriched = job.enrich_with_dimension_keys(
            df_silver_read, employee_col="employee_id", date_col="from_date"
        )

        df_gold = df_enriched.select(
            "dim_employee_key",
            "dim_date_key",
            "source_leave_id",
            "zoho_record_id",
            "approval_status",
            "date_of_request",
            "from_date",
            "to_date",
            "leave_type",
            "team_email_id",
            "leave_unit",
            "zuid_in_leave",
            "reason",
            "leave_start_time",
            "leave_end_time",
            "leave_count",
            current_timestamp().alias("dw_load_timestamp")
        ).filter(col("dim_employee_key").isNotNull())

        gold_count = df_gold.count()
        logger.info(f"Gold records to write: {gold_count}")
        job.write_to_gold(df_gold, "fact_leave", mode="overwrite")

        # ============================================================
        # STEP 5 — MYSQL: Incremental upsert by source_leave_id
        # ============================================================
        logger.info("=== MYSQL: Syncing Gold → MySQL (incremental upsert) ===")
        df_gold_read = job.read_from_uc(UC_GOLD)

        # Fetch existing source_leave_ids from MySQL (single unique key)
        jdbc_url = f"jdbc:mysql://{job.mysql_host}:{job.mysql_port}/{job.mysql_db}?useSSL=false&serverTimezone=UTC"
        existing_ids_df = (
            job.spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", f"(SELECT source_leave_id FROM {job.mysql_db}.{MYSQL_TABLE}) AS existing")
            .option("user", job.mysql_user)
            .option("password", job.mysql_password)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .load()
        )

        # Anti-join: only insert records NOT already in MySQL
        df_new = df_gold_read.join(
            existing_ids_df,
            on="source_leave_id",
            how="left_anti"
        )

        new_count = df_new.count()
        logger.info(f"New leave records for MySQL: {new_count}")

        if new_count > 0:
            job.write_to_mysql(df_new, MYSQL_TABLE, mode="append")
            logger.info(f"✅ {new_count} new leave records inserted into MySQL: {MYSQL_TABLE}")
        else:
            logger.info("No new leave records to insert into MySQL.")

        # ============================================================
        # STEP 6 — ARCHIVE: Move processed files to success folder
        # ============================================================
        job.archive_files(file_list, SOURCE_PATH, SUCCESS_PATH)
        logger.info("✅ Leave transformation complete.")

    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise e
    finally:
        job.stop()


if __name__ == "__main__":
    run_leave_transform()
