from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.functions import sha2, concat_ws, current_timestamp, lit, col, trim, when, expr, to_timestamp, to_date
from pyspark.sql.types import ArrayType, StringType, MapType, StructType, StructField
import logging
import sys
import os
import mysql.connector

# Add utils path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.shared_transformation import SharedTransformationJob

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_employee_transform():
    # Initialize job
    job = SharedTransformationJob(app_name="employee_scd2_ingestion")
    
    # Overriding container for this specific job
    job.container_name = "airbytecontainer"
    job.container_client = job._get_container_client()
    
    FOLDER_PATH = "new_employee_details/"
    MYSQL_TABLE = "dim_employee"

    try:
        # Pre-check: Ensure record_hash and dw_load_timestamp column exists
        conn = mysql.connector.connect(
            host=job.mysql_host,
            port=job.mysql_port,
            user=job.mysql_user,
            password=job.mysql_password,
            database=job.mysql_db,
            auth_plugin='mysql_native_password'
        )
        cursor = conn.cursor()
        cursor.execute(f"DESCRIBE {MYSQL_TABLE}")
        columns = [row[0] for row in cursor.fetchall()]
        
        # Add missing columns if needed (schema evolution)
        if 'record_hash' not in columns:
            logger.info(f"Adding record_hash column to {MYSQL_TABLE}...")
            cursor.execute(f"ALTER TABLE {MYSQL_TABLE} ADD COLUMN record_hash VARCHAR(256) AFTER employee_id")
        
        if 'dw_load_timestamp' not in columns:
             logger.info(f"Adding dw_load_timestamp column to {MYSQL_TABLE}...")
             cursor.execute(f"ALTER TABLE {MYSQL_TABLE} ADD COLUMN dw_load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP")

        conn.commit()
        cursor.close()
        conn.close()

        # 1. List Files
        file_list = job.list_files(FOLDER_PATH, ".jsonl")
        
        # Filter out failed files
        file_list = [f for f in file_list if "_failed_parse" not in os.path.basename(f)]
        
        if not file_list:
            logger.info("No employee files to process.")
            job.stop()
            return

        # 2. Read Files as Text (Stability/Performance Optimization)
        df_raw = job.read_files(file_list, download_locally=True, file_type="text")
        if df_raw is None:
            job.stop()
            return

        # 3. Bronze Layer: Store raw JSON lines as-is in Delta Lake
        logger.info("=== BRONZE: Writing raw employee data (as text) ===")
        job.write_to_bronze(df_raw.select(col("value").alias("raw_json")), "employee_raw")

        # 4. Silver Layer: Transform/Clean/Flat
        logger.info("=== SILVER: Reading Bronze and transforming ===")
        UC_BRONZE = "hr_analytics.bronze.employee_raw"
        df_bronze = job.read_from_uc(UC_BRONZE)
        bronze_count = df_bronze.count()
        logger.info(f"Loaded {bronze_count} records from Bronze.")
        
        if bronze_count > 0:
            sample_json = df_bronze.select("raw_json").first()[0]
            logger.info(f"Raw JSON Sample (first 200 chars): {sample_json[:200]}")
        
        # Extraction Logic + Preserve ingest_timestamp
        # We need to handle 3 cases:
        # 1. Payload is a single record (e.g. Airbyte line): {"EmployeeID": "...", ...}
        # 2. Payload is an Array of records: [{"EmployeeID": "..."}, ...]
        # 3. Payload is a Map of records (Zoho API): {"id1": {"EmployeeID": "..."}, ...}

        payload_col = F.coalesce(
            F.get_json_object(col("raw_json"), "$._airbyte_data.response.result"),
            F.get_json_object(col("raw_json"), "$.response.result"),
            F.get_json_object(col("raw_json"), "$.result"),
            F.get_json_object(col("raw_json"), "$._airbyte_data"),
            col("raw_json")
        )

        # Schema for a single employee record
        emp_struct_schema = """STRUCT<
            EmployeeID:STRING, Zoho_ID:STRING, ZUID:STRING, FirstName:STRING, LastName:STRING, 
            EmailID:STRING, Other_Email:STRING, Mobile:STRING, Work_phone:STRING, 
            Date_of_birth:STRING, Department:STRING, Designation:STRING, 
            Reporting_To:STRING, Role:STRING, Employee_type:STRING, Employeestatus:STRING, 
            Dateofjoining:STRING, Dateofexit:STRING, LocationName:STRING, 
            Work_location:STRING, AddedBy:STRING, AddedTime:STRING, 
            ModifiedBy:STRING, ModifiedTime:STRING, ApprovalStatus:STRING, Source_of_hire:STRING
        >"""

        # 1. Handle Single Records (Airbyte typical)
        df_single = df_bronze.withColumn("payload", payload_col) \
            .filter(F.get_json_object(F.col("payload"), "$.EmployeeID").isNotNull()) \
            .select(
                col("ingest_timestamp"),
                F.from_json(col("payload"), emp_struct_schema).alias("employee")
            )

        # 2. Handle Arrays (might be Array of Structs or Array of Maps)
        df_array = df_bronze.withColumn("payload", payload_col) \
            .filter(F.get_json_object(F.col("payload"), "$.EmployeeID").isNull() & F.col("payload").startswith("[")) \
            .select(
                col("ingest_timestamp"),
                F.explode(F.from_json(col("payload"), "ARRAY<STRING>")).alias("record_json")
            )
            
        # Recursive attempt: if record_json is still a map (like in the sample), we need to extract the values
        # Sample: {"140168...": [...]}
        df_array_fixed = df_array.select(
            col("ingest_timestamp"),
            F.when(
                F.get_json_object(col("record_json"), "$.EmployeeID").isNotNull(),
                F.array(col("record_json"))
            ).otherwise(
                # It's a map like {"id": [record]}, get the values
                F.map_values(F.from_json(col("record_json"), "MAP<STRING, STRING>"))
            ).alias("final_records")
        ).select(
            col("ingest_timestamp"),
            F.explode(col("final_records")).alias("inner_json")
        ).select(
            col("ingest_timestamp"),
            # If inner_json is an array (like in Zoho sample), take first element. If not, it's the record.
            F.when(col("inner_json").startswith("["), 
                   F.from_json(F.from_json(col("inner_json"), "ARRAY<STRING>").getItem(0), emp_struct_schema)
            ).otherwise(
                   F.from_json(col("inner_json"), emp_struct_schema)
            ).alias("employee")
        )

        # 3. Handle Maps (Zoho typical root response)
        df_map = df_bronze.withColumn("payload", payload_col) \
            .filter(F.get_json_object(F.col("payload"), "$.EmployeeID").isNull() & F.col("payload").startswith("{")) \
            .select(
                col("ingest_timestamp"),
                F.explode(F.from_json(col("payload"), "MAP<STRING, STRING>")).alias("zoho_record_id", "record_json")
            ).select(
                col("ingest_timestamp"),
                # Same logic: if value is array, take first.
                F.when(col("record_json").startswith("["),
                       F.from_json(F.from_json(col("record_json"), "ARRAY<STRING>").getItem(0), emp_struct_schema)
                ).otherwise(
                       F.from_json(col("record_json"), emp_struct_schema)
                ).alias("employee")
            )

        # Union paths
        df_exploded = df_single.unionByName(df_array_fixed, allowMissingColumns=True) \
                               .unionByName(df_map, allowMissingColumns=True) \
                               .filter(col("employee").isNotNull())

        logger.info(f"Records after dynamic extraction: {df_exploded.count()}")
        if df_exploded.count() == 0:
            logger.warning("No records extracted after explosion. Checking payload samples...")
            df_bronze.withColumn("payload", payload_col).select("payload").show(5, truncate=False)
         # Helper for safer access
        def get_emp_col(c):
            return F.col(f"employee.`{c}`")

        df_flat = df_exploded.select(
            col("ingest_timestamp"),
            get_emp_col("EmployeeID").alias("employee_id"),
            get_emp_col("Zoho_ID").alias("zoho_id"),
            get_emp_col("ZUID").alias("zuid"),
            get_emp_col("FirstName").alias("first_name"),
            get_emp_col("LastName").alias("last_name"),
            get_emp_col("EmailID").alias("email_id"),
            get_emp_col("Other_Email").alias("other_email"),
            get_emp_col("Mobile").alias("mobile"),
            get_emp_col("Work_phone").alias("work_phone"),
            get_emp_col("Date_of_birth").alias("date_of_birth"),
            get_emp_col("Department").alias("department"),
            get_emp_col("Designation").alias("designation"),
            get_emp_col("Reporting_To").alias("reporting_to"),
            get_emp_col("Role").alias("role"),
            get_emp_col("Employee_type").alias("employee_type"),
            get_emp_col("Employeestatus").alias("employee_status"),
            get_emp_col("Dateofjoining").alias("date_of_joining"),
            get_emp_col("Dateofexit").alias("date_of_exit"),
            get_emp_col("LocationName").alias("location_name"),
            get_emp_col("Work_location").alias("work_location"),
            get_emp_col("AddedBy").alias("added_by"),
            get_emp_col("AddedTime").alias("added_time"),
            get_emp_col("ModifiedBy").alias("modified_by"),
            get_emp_col("ModifiedTime").alias("modified_time"),
            get_emp_col("ApprovalStatus").alias("approval_status"),
            get_emp_col("Source_of_hire").alias("source_of_hire")
        )
        
        # Clean: Trim and Empty->Null
        df_cleaned = df_flat.select([
            F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c))).alias(c)
            for c in df_flat.columns
        ])
        
        # Derived Columns
        df_cleaned = df_cleaned \
            .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name"))) \
            .withColumn("is_billable", lit(0).cast("int")) \
            .withColumn("source_row_id", lit(None).cast("int"))

        # Date/Timestamp Casting
        date_cols = ["date_of_birth", "date_of_joining", "date_of_exit"]
        for c in date_cols:
            df_cleaned = df_cleaned.withColumn(
                c, 
                F.coalesce(
                    F.to_date(F.col(c), "dd-MMM-yyyy"),
                    F.to_date(F.col(c), "yyyy-MM-dd") # Fallback
                )
            )

        df_cleaned = df_cleaned \
            .withColumn("modified_time", (F.col("modified_time")/1000).cast("timestamp")) \
            .withColumn("added_time",
                F.when(
                    F.col("added_time").rlike(r"^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}:\d{2}$"),
                    F.to_timestamp("added_time","dd-MMM-yyyy HH:mm:ss")
                ).otherwise((F.col("added_time")/1000).cast("timestamp"))
            )

        # 6. CDC Hash
        scd_cols = [c for c in df_cleaned.columns if c not in ["employee_id", "dw_load_timestamp", "record_hash", "ingest_timestamp"]]
        df_cleaned = df_cleaned.withColumn(
            "record_hash",
            sha2(concat_ws("||", *scd_cols), 256)
        )
        
        # Intra-batch Deduplication: One record per employee_id (Keep latest)
        from pyspark.sql.window import Window
        window_spec = Window.partitionBy("employee_id").orderBy(col("ingest_timestamp").desc())
        df_silver = df_cleaned.filter(col("employee_id").isNotNull()) \
                              .withColumn("rn", F.row_number().over(window_spec)) \
                              .filter("rn = 1") \
                              .drop("rn", "ingest_timestamp")
        
        logger.info(f"Silver records ready: {df_silver.count()}")
        # Write to Silver
        job.write_to_silver(df_silver, "employee_cleaned")

        # 5. Gold Layer: Perform SCD2 logic within Unity Catalog
        df_silver = job.read_from_uc("hr_analytics.silver.employee_cleaned")
        gold_path = f"{job.DELTA_BASE}/gold/dim_employee"
        
        try:
            df_gold_current = job.read_from_uc("hr_analytics.gold.dim_employee") \
                                 .filter("is_current = 1") \
                                 .select(col("employee_id").alias("old_id"), col("record_hash").alias("old_hash"))
        except Exception:
            logger.info("Gold table not found. Assuming first run.")
            df_gold_current = job.spark.createDataFrame([], StructType([
                StructField("old_id", StringType(), True),
                StructField("old_hash", StringType(), True)
            ]))

        joined = df_silver.alias("new").join(df_gold_current.alias("old").hint("merge"), col("new.employee_id") == col("old.old_id"), "left")
        
        new_records_ids = joined.filter("old.old_id IS NULL").select("new.employee_id")
        changed_records_ids = joined.filter("old.old_id IS NOT NULL AND new.record_hash <> old.old_hash").select("new.employee_id")
        
        # 5a. Expire old records in Gold (Delta)
        if not changed_records_ids.rdd.isEmpty():
            from delta.tables import DeltaTable
            expire_list = [r.employee_id for r in changed_records_ids.distinct().collect()]
            if os.path.exists(gold_path.replace("file://", "")):
                dt = DeltaTable.forPath(job.spark, gold_path)
                dt.update(
                    condition=f"employee_id IN ({','.join([chr(39) + x + chr(39) for x in expire_list])}) AND is_current = 1",
                    set={"is_current": lit(0), "valid_to": current_timestamp()}
                )
                logger.info(f"Expired {len(expire_list)} old records in Gold (Delta).")

        # 5b. Insert new/changed records into Gold
        final_gold_insert = joined.filter("old.old_id IS NULL OR new.record_hash <> old.old_hash").select("new.*") \
            .withColumn("valid_from", current_timestamp()) \
            .withColumn("valid_to", lit(None).cast("timestamp")) \
            .withColumn("is_current", lit(1)) \
            .withColumn("dw_load_timestamp", current_timestamp())
            
        gold_insert_count = final_gold_insert.count()
        logger.info(f"Gold records to insert: {gold_insert_count}")

        if not final_gold_insert.rdd.isEmpty():
            job.write_to_gold(final_gold_insert, "dim_employee", mode="append")
            logger.info("Updated Gold dim_employee with new versions.")

        # 6. Sync Silver to MySQL (Ensures MySQL is catch-up even if Delta was ahead)
        jdbc_url = f"jdbc:mysql://{job.mysql_host}:{job.mysql_port}/{job.mysql_db}?useSSL=false&serverTimezone=UTC"
        MYSQL_TABLE = "dim_employee"
        
        try:
            existing_mysql_df = job.spark.read.format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", f"(SELECT employee_id, record_hash FROM {MYSQL_TABLE} WHERE is_current = 1) as t") \
                .option("user", job.mysql_user) \
                .option("password", job.mysql_password) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load() \
                .select(col("employee_id").alias("mysql_id"), col("record_hash").alias("mysql_hash"))
        except Exception as e:
            logger.info(f"MySQL table {MYSQL_TABLE} may be empty or not found: {e}")
            existing_mysql_df = job.spark.createDataFrame([], StructType([
                StructField("mysql_id", StringType(), True),
                StructField("mysql_hash", StringType(), True)
            ]))

        mysql_joined = df_silver.alias("new").join(existing_mysql_df.alias("old"), col("new.employee_id") == col("old.mysql_id"), "left")
        
        # New or Changed
        sync_to_mysql = mysql_joined.filter("old.mysql_id IS NULL OR new.record_hash <> old.mysql_hash").select("new.*") \
            .withColumn("valid_from", current_timestamp()) \
            .withColumn("valid_to", lit(None).cast("timestamp")) \
            .withColumn("is_current", lit(1)) \
            .withColumn("dw_load_timestamp", current_timestamp())
            
        mysql_sync_count = sync_to_mysql.count()
        logger.info(f"Records to sync to MySQL: {mysql_sync_count}")

        if mysql_sync_count > 0:
            # Expire old records in MySQL if they changed
            expire_ids_df = mysql_joined.filter("old.mysql_id IS NOT NULL AND new.record_hash <> old.mysql_hash").select(col("employee_id"))
            if not expire_ids_df.rdd.isEmpty():
                expire_ids = [r.employee_id for r in expire_ids_df.collect()]
                conn = mysql.connector.connect(
                    host=job.mysql_host, port=job.mysql_port, user=job.mysql_user,
                    password=job.mysql_password, database=job.mysql_db, auth_plugin='mysql_native_password'
                )
                cursor = conn.cursor()
                id_string = ",".join([f"'{x}'" for x in expire_ids])
                cursor.execute(f"UPDATE {MYSQL_TABLE} SET is_current = 0, valid_to = NOW() WHERE employee_id IN ({id_string}) AND is_current = 1")
                conn.commit()
                cursor.close(); conn.close()
                logger.info(f"Expired {len(expire_ids)} records in MySQL.")

            job.write_to_mysql(sync_to_mysql, MYSQL_TABLE, mode="append")
            logger.info(f"✅ Successfully synced {mysql_sync_count} records to MySQL.")
        else:
            logger.info("MySQL is already up to date.")

        # FINAL CHECK
        total_gold = job.read_from_uc("hr_analytics.gold.dim_employee").filter("is_current = 1").count()
        logger.info(f"🏁 Final SCD2 Status: Gold Delta={total_gold} records.")

        # Archive Success
        job.archive_files(file_list, FOLDER_PATH, "new_employee_details_success/")

    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise e
    finally:
        job.stop()

if __name__ == "__main__":
    run_employee_transform()
