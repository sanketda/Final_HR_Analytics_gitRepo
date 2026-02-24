
from pyspark.sql import SparkSession
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_spark_session():
    packages = [
        "io.delta:delta-spark_2.12:3.0.0",
        "io.unitycatalog:unitycatalog-spark_2.12:0.2.1",
        "org.apache.hadoop:hadoop-azure:3.3.4",
        "com.microsoft.azure:azure-storage:8.6.6"
    ]
    
    return (SparkSession.builder
            .appName("LakehouseTableSetup")
            .config("spark.jars.packages", ",".join(packages))
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.hr_analytics", "io.unitycatalog.spark.UCSingleCatalog")
            .config("spark.sql.catalog.hr_analytics.uri", "http://unity-catalog:8080")
            .config("spark.sql.defaultCatalog", "hr_analytics")
            .getOrCreate())

def setup_tables():
    spark = get_spark_session()
    
    try:
        logger.info("🚀 Starting Lakehouse Table Initialization (Explicity Names)...")
        
        # 1. Base Setup (Schemas)
        logger.info("Creating Bronze, Silver, Gold schemas if not exist...")
        spark.sql("CREATE SCHEMA IF NOT EXISTS hr_analytics.bronze")
        spark.sql("CREATE SCHEMA IF NOT EXISTS hr_analytics.silver")
        spark.sql("CREATE SCHEMA IF NOT EXISTS hr_analytics.gold")

        # 2. Gold - dim_employee (SCD Type 2)
        logger.info("Creating Gold Table: dim_employee...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS hr_analytics.gold.dim_employee (
                employee_id STRING,
                zoho_id STRING,
                zuid STRING,
                first_name STRING,
                last_name STRING,
                full_name STRING,
                email_id STRING,
                other_email STRING,
                mobile STRING,
                work_phone STRING,
                date_of_birth DATE,
                department STRING,
                designation STRING,
                reporting_to STRING,
                role STRING,
                employee_type STRING,
                employee_status STRING,
                date_of_joining DATE,
                date_of_exit DATE,
                location_name STRING,
                work_location STRING,
                added_by STRING,
                added_time TIMESTAMP,
                modified_by STRING,
                modified_time TIMESTAMP,
                approval_status STRING,
                source_of_hire STRING,
                is_billable INT,
                source_row_id INT,
                record_hash STRING,
                valid_from TIMESTAMP,
                valid_to TIMESTAMP,
                is_current INT,
                dw_load_timestamp TIMESTAMP
            ) USING DELTA
        """)

        # 3. Gold - fact_attendance (Zoho)
        logger.info("Creating Gold Table: fact_attendance...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS hr_analytics.gold.fact_attendance (
                dim_employee_key INT,
                dim_date_key INT,
                source_attendance_id STRING,
                work_date_str STRING,
                deviation_time STRING,
                first_in_building STRING,
                first_in_location STRING,
                last_out_building STRING,
                last_out_location STRING,
                shift_end_time_str STRING,
                shift_name STRING,
                zoho_status STRING,
                zoho_total_hours_str STRING,
                zoho_working_hour_str STRING,
                ercno LONG,
                shifttime_str STRING,
                first_in_ts TIMESTAMP,
                last_out_ts TIMESTAMP,
                work_minutes INT,
                total_minutes INT,
                dw_load_timestamp TIMESTAMP
            ) USING DELTA
        """)

        # 4. Gold - fact_attendance_biometric
        logger.info("Creating Gold Table: fact_attendance_biometric...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS hr_analytics.gold.fact_attendance_biometric (
                dim_employee_key INT,
                dim_date_key INT,
                source_bio_id STRING,
                name_in_bio STRING,
                shift_in_bio STRING,
                bio_in_time_str STRING,
                bio_out_time_str STRING,
                bio_work_dur_str STRING,
                ot_str STRING,
                bio_total_dur_str STRING,
                bio_status STRING,
                remarks STRING,
                work_date DATE,
                work_minutes INT,
                total_minutes INT,
                ot_minutes INT,
                dw_load_timestamp TIMESTAMP
            ) USING DELTA
        """)

        # 5. Gold - fact_leave
        logger.info("Creating Gold Table: fact_leave...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS hr_analytics.gold.fact_leave (
                dim_employee_key INT,
                dim_date_key INT,
                source_leave_id LONG,
                zoho_record_id STRING,
                approval_status STRING,
                date_of_request DATE,
                from_date DATE,
                to_date DATE,
                leave_type STRING,
                team_email_id STRING,
                leave_unit STRING,
                zuid_in_leave LONG,
                reason STRING,
                leave_start_time STRING,
                leave_end_time STRING,
                leave_count DECIMAL(4,1),
                dw_load_timestamp TIMESTAMP
            ) USING DELTA
        """)

        # 6. Gold - fact_timesheet
        logger.info("Creating Gold Table: fact_timesheet...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS hr_analytics.gold.fact_timesheet (
                dim_employee_key INT, dim_date_key INT, source_log_id INT, log_approval_status STRING, billed_status STRING, billing_status STRING, client_id STRING, description STRING, erecno STRING, from_time_bigint LONG, from_time_str STRING, hours_str STRING, minutes_int INT, is_delete_allowed TINYINT, is_edit_allowed TINYINT, is_push_to_zf TINYINT, is_push_to_zp TINYINT, is_pushed_to_qbo TINYINT, job_billable_status STRING, job_color STRING, job_id STRING, job_is_active TINYINT, job_is_completed INT, job_name STRING, project_id STRING, project_name STRING, task_name STRING, timelog_id STRING, timer_log TINYINT, to_time_bigint LONG, to_time_str STRING, total_time_bigint LONG, tt_input_type INT, type_col STRING, work_date DATE, dw_load_timestamp TIMESTAMP
            ) USING DELTA
        """)

        logger.info("✅ All Gold tables pre-created successfully in Unity Catalog!")
        
        # Verify
        logger.info("Verifying tables in gold schema:")
        spark.sql("SHOW TABLES IN hr_analytics.gold").show()

    except Exception as e:
        logger.error(f"❌ Table setup failed: {e}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    setup_tables()
