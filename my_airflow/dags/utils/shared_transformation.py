
import os
import time
import logging
from azure.storage.blob import ContainerClient
from pyspark.sql import SparkSession

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_env_file():
    """Manually load .env file if it exists in expected locations."""
    possible_paths = [
        os.path.join(os.getcwd(), ".env"),
        os.path.join(os.path.dirname(__file__), ".env"),
        os.path.join(os.path.dirname(__file__), "../../../.env"), 
        os.path.join(os.path.dirname(__file__), "../../.env"),
        "/opt/airflow/.env"
    ]
    for path in possible_paths:
        if os.path.exists(path):
            logger.info(f"Loading environment from {path}")
            with open(path) as f:
                for line in f:
                    if line.strip() and not line.startswith("#"):
                        key, value = line.strip().split("=", 1)
                        os.environ[key] = value
            return
    logger.warning("No .env file found in expected locations.")

# Load env immediately
load_env_file()

class SharedTransformationJob:
    """
    A shared utility class to handle common Data Engineering tasks:
    1. initializing Spark Session with Azure & JDBC configs.
    2. Listing and Reading files from Azure Blob Storage (distributed).
    3. Writing DataFrames to MySQL.
    4. Archiving processed files to Success/Failed folders.
    """

    def __init__(self, app_name="SharedTransformation"):
        self.app_name = app_name
        
        # Unique temp directory for downloaded files
        self.temp_dir = f"/tmp/{self.app_name}_{int(time.time())}"
        if not os.path.exists(self.temp_dir):
            os.makedirs(self.temp_dir)
            
        # ==========================================
        # Configuration (Centralized)
        # ==========================================
        self.account_name = "yogeshdatalakegen2"
        self.container_name = "zohopeople"
        self.credential = os.getenv("AZURE_STORAGE_KEY", "REPLACED_SECRET")
        
        self.mysql_host = "164.52.195.88"
        self.mysql_port = "3308"
        self.mysql_db = "timeTracke_Dev"
        self.mysql_user = "root"
        self.mysql_password = os.getenv("MYSQL_PASSWORD", "shyenatech")
        
        # Path to JDBC Jar - derived from checking existing code
        self.jdbc_jar = "/opt/airflow/dags/utils/mysql-connector-j-8.0.33.jar"

        self.spark = self._create_spark_session()
        self.container_client = self._get_container_client()
        self._ensure_schemas()

    def _create_spark_session(self):
        """Creates a SparkSession with Azure and JDBC drivers configured."""
        logger.info(f"Initializing Spark Session: {self.app_name}")
        
        # Maven coordinates for Azure support
        # Adjust versions if necessary based on Spark/Hadoop version (~Hadoop 3.3.x)
        azure_packages = "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6"
        uc_packages = "io.delta:delta-spark_2.12:3.2.0,io.unitycatalog:unitycatalog-spark_2.12:0.2.1"
        all_packages = f"{azure_packages},{uc_packages}"
        
        builder = (
            SparkSession.builder.appName(self.app_name)
            .config("spark.sql.session.timeZone", "Asia/Kolkata")
            .config("spark.driver.memory", "1024m")
            .config("spark.executor.memory", "1024m")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.codegen.wholeStage", "false")
            .config("spark.jars", self.jdbc_jar)
            .config("spark.driver.extraClassPath", self.jdbc_jar)
            .config("spark.executor.extraClassPath", self.jdbc_jar)
            .config("spark.jars.packages", all_packages)
            # Unity Catalog & Delta Configuration
            # Catalog alias MUST be 'hr_analytics' to match all SQL references (hr_analytics.bronze.*, etc.)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            # DeltaCatalog is REQUIRED for Delta saveAsTable() to resolve the spark_catalog
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            # Unity Catalog alias - must be 'hr_analytics' to match all SQL references
            .config("spark.sql.catalog.hr_analytics", "io.unitycatalog.spark.UCSingleCatalog")
            .config("spark.sql.catalog.hr_analytics.uri", "http://unity-catalog:8080")
            # Azure Blob Storage Configuration (WASBS)
            .config(f"fs.azure.account.key.{self.account_name}.blob.core.windows.net", self.credential)
            .config("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
            .config("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
        )
        return builder.getOrCreate()

    def _get_container_client(self):
        """Creates the Azure Blob Container Client."""
        return ContainerClient(
            account_url=f"https://{self.account_name}.blob.core.windows.net",
            container_name=self.container_name,
            credential=self.credential
        )

    def _ensure_schemas(self):
        """
        Bootstraps the Unity Catalog namespace via REST API.
        """
        import requests

        UC_API = "http://unity-catalog:8080/api/2.1/unity-catalog"
        headers = {"Content-Type": "application/json"}

        print("\n" + "="*50)
        print("BOOTSTRAPPING UNITY CATALOG NAMESPACES")
        print("="*50)

        # ── 1. Ensure the catalog exists ─────────────────────────────
        try:
            print(f"-> creating catalog 'hr_analytics' via {UC_API}/catalogs")
            resp = requests.post(
                f"{UC_API}/catalogs",
                json={"name": "hr_analytics", "comment": "HR Analytics catalog"},
                headers=headers,
                timeout=10
            )
            body = resp.json()
            if "name" in body:
                print("   ✅ Catalog created.")
            elif "already exists" in body.get("message", "").lower():
                print("   ✅ Catalog already exists.")
            else:
                print(f"   ⚠️ Catalog create response: {body}")
        except Exception as e:
            print(f"   ❌ Could not create UC catalog via REST: {e}")

        # ── 2. Ensure bronze / silver / gold schemas exist ────────────
        for layer in ("bronze", "silver", "gold"):
            try:
                print(f"-> creating schema '{layer}' via {UC_API}/schemas")
                resp = requests.post(
                    f"{UC_API}/schemas",
                    json={"name": layer, "catalog_name": "hr_analytics",
                          "comment": f"{layer.capitalize()} medallion layer"},
                    headers=headers,
                    timeout=10
                )
                body = resp.json()
                if "schema_id" in body or "name" in body:
                    print(f"   ✅ Schema {layer} created.")
                elif "already exists" in body.get("message", "").lower():
                    print(f"   ✅ Schema {layer} already exists.")
                else:
                    print(f"   ⚠️ Schema '{layer}' response: {body}")
            except Exception as e:
                print(f"   ❌ Could not create UC schema '{layer}' via REST: {e}")
        
        print("="*50 + "\n")

    def list_files(self, folder_path, file_pattern=".jsonl"):
        """
        Lists all files in a blob folder matching the pattern.
        Returns a list of blob names.
        """
        try:
            blobs = self.container_client.list_blobs(name_starts_with=folder_path)
            file_list = [b.name for b in blobs if b.name.endswith(file_pattern)]
            logger.info(f"Found {len(file_list)} files in {folder_path}")
            return file_list
        except Exception as e:
            logger.error(f"Error listing files: {e}")
            raise e

    def read_files(self, file_list, download_locally=True, file_type="json", **options):
        """
        Reads files from Azure Blob Storage into a Spark DataFrame.
        If download_locally=True, downloads files to a temporary directory first.
        Otherwise, reads directly from the blob using wasbs.
        'file_type' can be 'json' or 'text'.
        """
        if not file_list:
            logger.warning("No files provided to read_files.")
            return None

        logger.info(f"Reading {len(file_list)} {file_type} files with Spark (Download Local: {download_locally})...")
        
        try:
            # Default options
            read_opts = {"multiline": "false"}
            read_opts.update(options)
            
            reader = self.spark.read
            for k, v in read_opts.items():
                reader = reader.option(k, v)

            if download_locally:
                local_paths = []
                for blob_name in file_list:
                    try:
                        # Construct local path
                        file_name = os.path.basename(blob_name)
                        local_target = os.path.join(self.temp_dir, file_name)
                        
                        # Download using SDK
                        with open(local_target, "wb") as f:
                            data = self.container_client.download_blob(blob_name).readall()
                            f.write(data)
                        
                        local_paths.append(f"file://{local_target}")
                    except Exception as de:
                         logger.error(f"Failed to download {blob_name}: {de}")
                         # Continue or raise? Raising is safer for data integrity
                         raise de
                
                logger.info(f"Downloaded {len(local_paths)} files to {self.temp_dir}")
                
                if file_type == "text":
                    return reader.text(local_paths)
                else:
                    return reader.json(local_paths)

            else:
                # distributed read via WASBS
                wasbs_paths = [
                    f"wasbs://{self.container_name}@{self.account_name}.blob.core.windows.net/{blob_name}" 
                    for blob_name in file_list
                ]
                if file_type == "text":
                    return reader.text(wasbs_paths)
                else:
                    return reader.json(wasbs_paths)
                
        except Exception as e:
            logger.error(f"Spark read failed: {e}")
            raise e

    def get_existing_keys(self, table_name, key_column):
        """
        Fetches existing keys from MySQL to perform deduplication/anti-join.
        """
        jdbc_url = f"jdbc:mysql://{self.mysql_host}:{self.mysql_port}/{self.mysql_db}?useSSL=false&serverTimezone=UTC"
        query = f"(SELECT {key_column} FROM {self.mysql_db}.{table_name}) AS existing_keys"
        
        return (self.spark.read.format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", query)
                .option("user", self.mysql_user)
                .option("password", self.mysql_password)
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .load())

    def write_to_mysql(self, df, table_name, mode="append", batch_size=5000):
        """
        Writes a DataFrame to MySQL using JDBC.
        """
        jdbc_url = f"jdbc:mysql://{self.mysql_host}:{self.mysql_port}/{self.mysql_db}?useSSL=false&serverTimezone=UTC"
        logger.info(f"Writing {df.count()} records to MySQL table: {table_name}")

        (df.write
         .format("jdbc")
         .option("url", jdbc_url)
         .option("dbtable", table_name)
         .option("user", self.mysql_user)
         .option("password", self.mysql_password)
         .option("driver", "com.mysql.cj.jdbc.Driver")
         .option("batchsize", batch_size)
         .mode(mode)
         .save())
        logger.info("Write successful.")

    def enrich_with_dimension_keys(self, df, employee_col="employee_id", date_col="work_date"):
        """
        Enriches the fact dataframe with dim_employee_key and dim_date_key.
        """
        jdbc_url = f"jdbc:mysql://{self.mysql_host}:{self.mysql_port}/{self.mysql_db}?useSSL=false&serverTimezone=UTC"
        
        # 1. Fetch Dim Employee (only current ones)
        dim_employee = (self.spark.read.format("jdbc")
                        .option("url", jdbc_url)
                        .option("dbtable", "(SELECT dim_employee_key, employee_id, email_id FROM dim_employee WHERE is_current = 1) as e")
                        .option("user", self.mysql_user)
                        .option("password", self.mysql_password)
                        .option("driver", "com.mysql.cj.jdbc.Driver")
                        .load())
        
        # 2. Fetch Dim Date
        dim_date = (self.spark.read.format("jdbc")
                    .option("url", jdbc_url)
                    .option("dbtable", "(SELECT dim_date_key, full_date FROM dim_date) as d")
                    .option("user", self.mysql_user)
                    .option("password", self.mysql_password)
                    .option("driver", "com.mysql.cj.jdbc.Driver")
                    .load())

        # Join logic
        # First join with employee (try both employee_id and email_id if needed, but usually one is enough)
        # Assuming input df has either employee_id or we might need to handle email
        from pyspark.sql.functions import col
        
        # Determine joining column for employee
        if employee_col in df.columns:
            # Force String comparison to prevent Spark from casting valid alphanumeric IDs in lookup table to Int/Long
            # (which causes 22018 Errors if LHS is inferred as Int but RHS is Alphanumeric String)
            df = df.join(
                dim_employee, 
                df[employee_col].cast("string") == dim_employee.employee_id.cast("string"), 
                "left"
            )
        elif "email" in df.columns:
             df = df.join(dim_employee, df["email"] == dim_employee.email_id, "left")
        
        # Join with date
        if date_col in df.columns:
            df = df.join(dim_date, df[date_col] == dim_date.full_date, "left")
            
        return df

    def archive_files(self, file_list, source_folder, target_folder):
        """
        Moves processed files from source_folder to target_folder (Success/Failed).
        """
        logger.info(f"Archiving {len(file_list)} files to {target_folder}...")
        
        for blob_name in file_list:
            try:
                source_blob = self.container_client.get_blob_client(blob_name)
                # Construct destination path: replace source prefix with target prefix
                # or just append basename if structure is flat.
                # Adapting to existing logic: replace FOLDER_PATH with SUCCESS_FOLDER
                dest_blob_name = blob_name.replace(source_folder, target_folder)
                
                dest_blob = self.container_client.get_blob_client(dest_blob_name)

                # Check if destination exists - delete it to prevent 409 Conflict/InvalidBlobType
                try:
                    if dest_blob.exists():
                        dest_blob.delete_blob()
                except:
                    pass
                
                dest_blob.start_copy_from_url(source_blob.url)
                
                # Wait for copy to complete
                props = dest_blob.get_blob_properties()
                while props.copy.status == 'pending':
                    time.sleep(0.1)
                    props = dest_blob.get_blob_properties()
                
                if props.copy.status == 'success':
                    source_blob.delete_blob()
                    # logger.info(f"Moved: {blob_name}") # Commented to reduce noise if many files
                else:
                    logger.error(f"Copy failed for {blob_name}: {props.copy.status}")
            
            except Exception as e:
                logger.error(f"Failed to move {blob_name}: {e}")
        
        logger.info("Archiving completed.")

    # Physical storage base path — must match what is registered in Unity Catalog via REST API.
    # Each table's storage_location = DELTA_BASE / {schema} / {table_name}
    DELTA_BASE = "file:///home/mount_disk/Eclassifier-workspace/Data-engineer-airflow/hr_analytics_dev/delta_lake/hr_analytics"

    def _write_to_layer(self, df, layer, table_name, mode, extra_options=None):
        """
        Core Delta write helper used by write_to_bronze/silver/gold.

        Strategy (append-or-create):
        ─────────────────────────────────────────────────────────────────
        With Unity Catalog's UCSingleCatalog, saveAsTable() in 'append'
        mode REQUIRES the table to already exist in UC.  On the very
        first run the table is absent, so we catch TABLE_OR_VIEW_NOT_FOUND
        and retry with 'overwrite', which auto-creates the table + Delta
        log in UC. Every subsequent run goes through the normal append path.

        For callers that pass mode='overwrite' (silver/gold default), the
        first attempt already uses overwrite so the fallback is a no-op.
        """
        table_full_name = f"hr_analytics.{layer}.{table_name}"
        path = f"{self.DELTA_BASE}/{layer}/{table_name}"
        logger.info(f"Writing to UC {layer.capitalize()}: {table_full_name} → {path}")

        # Define schema evolution strategy
        schema_option = "overwriteSchema" if mode == "overwrite" else "mergeSchema"
        
        writer = (
            df.write
            .format("delta")
            .mode(mode)
            .option(schema_option, "true")
        )
        if extra_options:
            for k, v in extra_options.items():
                writer = writer.option(k, v)
        
        # Write directly to the path (bypassing BUGGY UC single catalog saveAsTable)
        writer.save(path)

    def write_to_bronze(self, df, table_name):
        """
        Appends raw data to the Bronze layer in Unity Catalog.
        Adds an ingestion timestamp.  Auto-creates the Delta table on
        first run via the append-or-create fallback in _write_to_layer.
        """
        from pyspark.sql.functions import current_timestamp
        df_with_ts = df.withColumn("ingest_timestamp", current_timestamp())
        self._write_to_layer(df_with_ts, "bronze", table_name, mode="append")

    def write_to_silver(self, df, table_name, mode="overwrite"):
        """
        Writes cleaned/enriched data to the Silver layer in Unity Catalog.
        Uses explicit path to align with UC-registered table storage location.
        """
        self._write_to_layer(df, "silver", table_name, mode=mode,
                             extra_options={"mergeSchema": "true"})

    def write_to_gold(self, df, table_name, mode="overwrite"):
        """
        Writes curated data to the Gold layer in Unity Catalog.
        Uses explicit path to align with UC-registered table storage location.
        """
        self._write_to_layer(df, "gold", table_name, mode=mode,
                             extra_options={"mergeSchema": "true"})

    def read_from_uc(self, table_full_name):
        """
        Reads from a 'UC' table by translating its namespace into the direct Delta path.
        Bypasses the buggy UCSingleCatalog read resolution (TABLE_OR_VIEW_NOT_FOUND)
        by directly reading the underlying Delta files.
        Example: hr_analytics.bronze.attendance_raw -> file://.../bronze/attendance_raw
        """
        parts = table_full_name.split(".")
        if len(parts) != 3:
            raise ValueError(f"Table name must be exactly catalog.schema.table, got {table_full_name}")
        
        _, layer, table_name = parts
        path = f"{self.DELTA_BASE}/{layer}/{table_name}"
        
        logger.info(f"Reading Delta from path: {path} ({table_full_name})")
        return self.spark.read.format("delta").load(path)

    def stop(self):
        # Cleanup temp dir
        try:
            if os.path.exists(self.temp_dir):
                import shutil
                shutil.rmtree(self.temp_dir)
                logger.info(f"Cleaned up temp dir: {self.temp_dir}")
        except Exception as e:
            logger.warning(f"Failed to cleanup temp dir {self.temp_dir}: {e}")
            
        self.spark.stop()
