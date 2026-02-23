from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, row_number, current_timestamp, split
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


def run_attendance_transform():
    """
    Medallion Architecture: Bronze → Silver → Gold → MySQL
    -------------------------------------------------------
    Bronze : Raw JSON from Azure Blob (attendance_raw)
    Silver : Cleaned, typed, deduplicated (attendance_cleaned)
    Gold   : Dimension-enriched fact table (fact_attendance)
    MySQL  : Incremental upsert into fact_attendance_zoho
    """
    job = SharedTransformationJob(app_name="transformAttendance")

    FOLDER_PATH   = "Zoho_leave/raw/Final_sheet_attendance/"
    SUCCESS_FOLDER = "Zoho_leave/success_files/attendance/"
    UC_BRONZE = "hr_analytics.bronze.attendance_raw"
    UC_SILVER = "hr_analytics.silver.attendance_cleaned"
    UC_GOLD   = "hr_analytics.gold.fact_attendance"
    MYSQL_TABLE   = "fact_attendance_zoho"

    def time_to_minutes(col_name):
        """Converts 'HH:MM' string column to total integer minutes."""
        return (
            split(col(col_name), ":")[0].cast("int") * 60 +
            split(col(col_name), ":")[1].cast("int")
        )

    try:
        # ============================================================
        # STEP 1: Read source files from Azure Blob Storage
        # ============================================================
        file_list = job.list_files(FOLDER_PATH, ".jsonl")
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
        # STEP 2 — BRONZE: Store raw JSON as-is in Delta Lake
        # ============================================================
        logger.info("=== BRONZE: Writing raw attendance data (as text) ===")
        job.write_to_bronze(df_raw.select(col("value").alias("raw_json")), "attendance_raw")

        # ============================================================
        # STEP 3 — SILVER: Flatten, clean, type-cast
        # ============================================================
        logger.info("=== SILVER: Reading Bronze and transforming ===")
        df_bronze = job.read_from_uc(UC_BRONZE)

        # Detect and extract data from the raw JSON string
        # Preserve ingest_timestamp for version-based deduplication
        df_exploded = df_bronze.select(
            col("ingest_timestamp"),
            F.get_json_object(col("raw_json"), "$._airbyte_ab_id").alias("source_attendance_id"),
            F.explode(
                F.from_json(
                    F.coalesce(
                        F.get_json_object(col("raw_json"), "$._airbyte_data.response.result"),
                        # Fallback if _airbyte_data is present but not nested, or data is at root
                        F.get_json_object(col("raw_json"), "$._airbyte_data"),
                        col("raw_json")
                    ),
                    "ARRAY<STRUCT<Date:STRING, DeviationTime:STRING, FirstIn:STRING, LastOut:STRING, TotalHours:STRING, email:STRING, employee_id:STRING, ercno:STRING, shifttime:STRING, working_hour:STRING, FirstIn_Building:STRING, FirstIn_Location:STRING, LastOut_Building:STRING, LastOut_Location:STRING, ShiftEndTime:STRING, ShiftName:STRING, Status:STRING>>"
                )
            ).alias("data")
        ).filter(col("data").isNotNull())

        # Extract all fields from the data struct
        df_clean = df_exploded.select(
            col("ingest_timestamp"),
            col("source_attendance_id"),
            col("data.Date").alias("work_date_str"),
            col("data.DeviationTime").alias("deviation_time"),
            col("data.FirstIn").alias("FirstIn"),
            col("data.FirstIn_Building").alias("first_in_building"),
            col("data.FirstIn_Location").alias("first_in_location"),
            col("data.LastOut").alias("LastOut"),
            col("data.LastOut_Building").alias("last_out_building"),
            col("data.LastOut_Location").alias("last_out_location"),
            col("data.ShiftEndTime").alias("shift_end_time_str"),
            col("data.ShiftName").alias("shift_name"),
            col("data.Status").alias("zoho_status"),
            col("data.TotalHours").alias("zoho_total_hours_str"),
            col("data.email").alias("email"),
            col("data.employee_id").alias("employee_id"),
            col("data.ercno").alias("ercno_raw"),
            col("data.shifttime").alias("shifttime_str"),
            col("data.working_hour").alias("zoho_working_hour_str")
        )

        # Safe-cast ercno (may be alphanumeric strings in some records)
        df_clean = df_clean \
            .withColumn("ercno", F.expr("try_cast(ercno_raw as long)")) \
            .drop("ercno_raw")

        # Parse timestamps (multiple formats for robustness)
        ts_formats = ["dd-MMM-yyyy hh:mm a", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss"]
        dt_formats = ["dd-MMM-yyyy", "yyyy-MM-dd"]

        df_clean = df_clean \
            .withColumn("first_in_ts",
                F.coalesce(*[F.expr(f'to_timestamp(trim(FirstIn), "{f}")') for f in ts_formats])
            ) \
            .withColumn("last_out_ts",
                F.coalesce(*[F.expr(f'to_timestamp(trim(LastOut), "{f}")') for f in ts_formats])
            ) \
            .drop("FirstIn", "LastOut")

        df_clean = df_clean.withColumn(
            "work_date",
            F.coalesce(*[F.expr(f'to_date(trim(work_date_str), "{f}")') for f in dt_formats])
        )

        # Drop records with no valid work_date
        df_clean = df_clean.filter(col("work_date").isNotNull())

        # Compute worked minutes from HH:MM strings
        df_clean = df_clean \
            .withColumn("work_minutes",  time_to_minutes("zoho_working_hour_str")) \
            .withColumn("total_minutes", time_to_minutes("zoho_total_hours_str"))

        # Deduplicate: keep latest record per (employee, date) based on ingestion time
        window_spec = Window.partitionBy("employee_id", "work_date").orderBy(col("ingest_timestamp").desc())
        df_silver = df_clean \
            .withColumn("row_num", row_number().over(window_spec)) \
            .filter(col("row_num") == 1) \
            .drop("row_num", "ingest_timestamp")

        logger.info(f"Silver records after dedup: {df_silver.count()}")
        job.write_to_silver(df_silver, "attendance_cleaned")

        # ============================================================
        # STEP 4 — GOLD: Enrich with dimension keys, build fact table
        # ============================================================
        logger.info("=== GOLD: Reading Silver and enriching with dim keys ===")
        df_silver_read = job.read_from_uc(UC_SILVER)
        df_enriched = job.enrich_with_dimension_keys(
            df_silver_read, employee_col="employee_id", date_col="work_date"
        )

        df_gold = df_enriched.select(
            "dim_employee_key",
            "dim_date_key",
            F.expr("xxhash64(source_attendance_id)").alias("source_attendance_id"),
            "work_date_str",
            "deviation_time",
            "first_in_building",
            "first_in_location",
            "last_out_building",
            "last_out_location",
            "shift_end_time_str",
            "shift_name",
            "zoho_status",
            "zoho_total_hours_str",
            col("zoho_working_hour_str"),
            col("ercno"),
            col("shifttime_str"),
            "first_in_ts",
            "last_out_ts",
            "work_minutes",
            "total_minutes",
            current_timestamp().alias("dw_load_timestamp")
        ).filter(col("dim_employee_key").isNotNull())

        gold_count = df_gold.count()
        logger.info(f"Gold records to write: {gold_count}")
        # Overwrite: attendance is a full-refresh per run (upsert logic handled in MySQL step)
        job.write_to_gold(df_gold, "fact_attendance", mode="overwrite")

        # ============================================================
        # STEP 5 — MYSQL: Incremental upsert (delete-insert by date)
        # ============================================================
        logger.info("=== MYSQL: Syncing Gold → MySQL (incremental upsert) ===")
        df_gold_read = job.read_from_uc(UC_GOLD)

        # Collect distinct work_date_str values in this batch to delete stale MySQL rows
        batch_dates = [
            r.work_date_str
            for r in df_gold_read.select("work_date_str").distinct().collect()
            if r.work_date_str is not None
        ]

        if batch_dates:
            import mysql.connector
            conn = mysql.connector.connect(
                host=job.mysql_host,
                port=int(job.mysql_port),
                user=job.mysql_user,
                password=job.mysql_password,
                database=job.mysql_db,
                auth_plugin="mysql_native_password"
            )
            cursor = conn.cursor()
            placeholders = ",".join(["%s"] * len(batch_dates))
            cursor.execute(
                f"DELETE FROM {MYSQL_TABLE} WHERE work_date_str IN ({placeholders})",
                batch_dates
            )
            deleted = cursor.rowcount
            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"Deleted {deleted} stale MySQL rows for {len(batch_dates)} dates before re-insert.")

        # Insert all Gold rows for this batch
        if not df_gold_read.rdd.isEmpty():
            job.write_to_mysql(df_gold_read, MYSQL_TABLE, mode="append")
            logger.info(f"✅ {df_gold_read.count()} records inserted into MySQL: {MYSQL_TABLE}")

        # ============================================================
        # STEP 6 — ARCHIVE: Move processed files to success folder
        # ============================================================
        job.archive_files(file_list, FOLDER_PATH, SUCCESS_FOLDER)
        logger.info("✅ Attendance transformation complete.")

    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise e
    finally:
        job.stop()


if __name__ == "__main__":
    run_attendance_transform()
