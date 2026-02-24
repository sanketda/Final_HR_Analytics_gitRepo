from pyspark.sql import functions as F
from pyspark.sql.functions import col, explode, lit, to_date, concat_ws, when, trim, current_timestamp, expr, to_json, get_json_object
from pyspark.sql.types import IntegerType, LongType, StringType, NullType
import logging
import sys
import os

# Add utils path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.shared_transformation import SharedTransformationJob

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_zoho_timesheet_transform():
    job = SharedTransformationJob(app_name="transformtimesheet")
    
    RAW_PATH = "Zoho_timesheet/raw/new_timesheet/"
    PASSED_PATH = "Zoho_timesheet/passed/"
    MYSQL_TABLE = "fact_timesheet"

    try:
        # 1. List Files
        file_list = job.list_files(RAW_PATH)
        if not file_list:
            logger.info("No files to process.")
            job.stop()
            return

        # 2. Read Files as Text (Stability/Performance Optimization)
        df_raw = job.read_files(file_list, download_locally=True, file_type="text")
        if df_raw is None:
            job.stop()
            return

        # 3. Bronze Layer: Store raw JSON lines as-is in Delta Lake
        logger.info("=== BRONZE: Writing raw timesheet data (as text) ===")
        job.write_to_bronze(df_raw.select(col("value").alias("raw_json")), "timesheet_raw")

        # 4. Silver Layer: Transform/Clean/Flat
        logger.info("=== SILVER: Reading Bronze and transforming ===")
        df_bronze = job.read_from_uc("hr_analytics.bronze.timesheet_raw")
        
        # DEBUG: Show raw bronze content
        logger.info("Raw Bronze content preview:")
        df_bronze.show(5, truncate=False)

        # Safely extract records from the nested 'response.result' array
        # Preserve ingest_timestamp for deduplication
        df_exploded = df_bronze.select(
            col("ingest_timestamp"),
            F.explode(
                F.from_json(
                    F.coalesce(
                        F.get_json_object(col("raw_json"), "$._airbyte_data.response.result"),
                        F.get_json_object(col("raw_json"), "$.response.result"),
                        F.get_json_object(col("raw_json"), "$.result")
                    ),
                    "ARRAY<STRING>"
                )
            ).alias("result_json")
        ).filter(col("result_json").isNotNull())

        logger.info(f"Records after explosion: {df_exploded.count()}")
        df_exploded.show(5, truncate=False)

        df_flat = df_exploded.select(
            col("ingest_timestamp"),
            get_json_object("result_json", "$.approvalStatus").alias("log_approval_status"),
            get_json_object("result_json", "$.billedStatus").alias("billed_status"),
            get_json_object("result_json", "$.billingStatus").alias("billing_status"),
            get_json_object("result_json", "$.clientId").alias("client_id"),
            get_json_object("result_json", "$.description").alias("description"),
            get_json_object("result_json", "$.erecno").alias("erecno"),
            get_json_object("result_json", "$.fromTime").cast("bigint").alias("from_time_bigint"),
            get_json_object("result_json", "$.fromTimeInTimeFormat").alias("from_time_str"),
            get_json_object("result_json", "$.hours").alias("hours_str"),
            get_json_object("result_json", "$.hoursInMins").cast("int").alias("minutes_int"),
            get_json_object("result_json", "$.isDeleteAllowed").cast("tinyint").alias("is_delete_allowed"),
            get_json_object("result_json", "$.isEditAllowed").cast("tinyint").alias("is_edit_allowed"),
            get_json_object("result_json", "$.isPushAllowToZF").cast("tinyint").alias("is_push_to_zf"),
            get_json_object("result_json", "$.isPushAllowToZP").cast("tinyint").alias("is_push_to_zp"),
            get_json_object("result_json", "$.isTimelogPushedToQBO").cast("tinyint").alias("is_pushed_to_qbo"),
            get_json_object("result_json", "$.jobBillableStatus").alias("job_billable_status"),
            get_json_object("result_json", "$.jobColor").alias("job_color"),
            get_json_object("result_json", "$.jobId").alias("job_id"),
            get_json_object("result_json", "$.jobIsActive").cast("tinyint").alias("job_is_active"),
            get_json_object("result_json", "$.jobIsCompleted").cast("int").alias("job_is_completed"),
            get_json_object("result_json", "$.jobName").alias("job_name"),
            get_json_object("result_json", "$.projectId").alias("project_id"),
            get_json_object("result_json", "$.projectName").alias("project_name"),
            get_json_object("result_json", "$.taskName").alias("task_name"),
            get_json_object("result_json", "$.timelogId").alias("timelog_id"),
            get_json_object("result_json", "$.timerLog").cast("tinyint").alias("timer_log"),
            get_json_object("result_json", "$.toTime").cast("bigint").alias("to_time_bigint"),
            get_json_object("result_json", "$.toTimeInTimeFormat").alias("to_time_str"),
            get_json_object("result_json", "$.totaltime").cast("bigint").alias("total_time_bigint"),
            get_json_object("result_json", "$.tt_inputType").cast("int").alias("tt_input_type"),
            get_json_object("result_json", "$.type").alias("type_col"),
            get_json_object("result_json", "$.workDate").alias("work_date_str"),
            get_json_object("result_json", "$.employeeMailId").alias("email"),
            get_json_object("result_json", "$.timelogId").cast("int").alias("source_log_id")
        )
        
        df_clean = df_flat.withColumn(
             "work_date",
             F.coalesce(*[F.expr(f'to_date(trim(work_date_str), "{f}")') for f in ["dd-MMM-yyyy", "yyyy-MM-dd"]])
        ).drop("work_date_str").filter(col("work_date").isNotNull())

        # ---- Deduplication: Keep only the latest ingest per timelog_id ----
        from pyspark.sql.window import Window
        window_spec = Window.partitionBy("timelog_id").orderBy(col("ingest_timestamp").desc())
        df_silver = df_clean \
            .withColumn("row_num", F.row_number().over(window_spec)) \
            .filter(col("row_num") == 1) \
            .drop("row_num", "ingest_timestamp")

        logger.info(f"Silver records after dedup: {df_silver.count()}")
        df_silver.show(5, truncate=False)

        # Write to Silver
        job.write_to_silver(df_silver, "timesheet_cleaned")

        # 5. Gold Layer: Enrich with Dimension Keys
        df_silver = job.read_from_uc("hr_analytics.silver.timesheet_cleaned")
        df_enriched = job.enrich_with_dimension_keys(df_silver, employee_col=None, date_col="work_date")

        # Final Select for Gold/Fact
        df_gold = df_enriched.select(
            "dim_employee_key",
            "dim_date_key",
            "source_log_id",
            "log_approval_status",
            "billed_status",
            "billing_status",
            "client_id",
            "description",
            "erecno",
            "from_time_bigint",
            "from_time_str",
            "hours_str",
            "minutes_int",
            "is_delete_allowed",
            "is_edit_allowed",
            "is_push_to_zf",
            "is_push_to_zp",
            "is_pushed_to_qbo",
            "job_billable_status",
            "job_color",
            "job_id",
            "job_is_active",
            "job_is_completed",
            "job_name",
            "project_id",
            "project_name",
            "task_name",
            "timelog_id",
            "timer_log",
            "to_time_bigint",
            "to_time_str",
            "total_time_bigint",
            "tt_input_type",
            "type_col",
            "work_date",
            current_timestamp().alias("dw_load_timestamp")
        ).filter(col("dim_employee_key").isNotNull())

        logger.info(f"Gold records ready to write: {df_gold.count()}")

        # Write to Gold
        job.write_to_gold(df_gold, "fact_timesheet")

        # 6. Sync Gold to MySQL (Incremental Load)
        df_final = job.read_from_uc("hr_analytics.gold.fact_timesheet")
        
        existing_keys_df = job.get_existing_keys(MYSQL_TABLE, "timelog_id")
        df_new = df_final.join(existing_keys_df, "timelog_id", "left_anti")

        if not df_new.rdd.isEmpty():
            records_to_insert = df_new.count()
            job.write_to_mysql(df_new, MYSQL_TABLE)
            logger.info(f"✅ {records_to_insert} new timesheet records inserted into MySQL from Gold layer")

        # 7. Archive
        job.archive_files(file_list, RAW_PATH, PASSED_PATH)

    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise e
    finally:
        job.stop()

if __name__ == "__main__":
    run_zoho_timesheet_transform()
