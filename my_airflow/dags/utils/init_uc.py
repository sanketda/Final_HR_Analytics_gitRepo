
from pyspark.sql import SparkSession
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_spark_session(app_name="InitUnityCatalog"):
    """
    Creates a SparkSession with Unity Catalog & Delta configurations for initialization.
    """
    packages = [
        "io.delta:delta-spark_2.12:3.0.0",
        "io.unitycatalog:unitycatalog-spark_2.12:0.2.1",
        "org.apache.hadoop:hadoop-azure:3.3.4",
        "com.microsoft.azure:azure-storage:8.6.6"
    ]
    
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.jars.packages", ",".join(packages))
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.uc", "io.unitycatalog.spark.UCSingleCatalog")
            .config("spark.sql.catalog.uc.uri", "http://unity-catalog:8080")
            .config("spark.sql.defaultCatalog", "uc")
            .getOrCreate())

def init_catalog():
    spark = get_spark_session()
    
    try:
        logger.info("Initializing Unity Catalog structure...")
        
        # 1. Create Catalog
        logger.info("Creating Catalog 'hr_analytics'...")
        spark.sql("CREATE CATALOG IF NOT EXISTS hr_analytics")
        
        # 2. Create Schemas (Medallion Layers)
        schemas = ["bronze", "silver", "gold"]
        for schema in schemas:
            logger.info(f"Creating Schema 'hr_analytics.{schema}'...")
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS hr_analytics.{schema}")
            
        logger.info("Unity Catalog initialization complete!")
        
        # Show results
        spark.sql("SHOW CATALOGS").show()
        spark.sql("USE CATALOG hr_analytics")
        spark.sql("SHOW SCHEMAS").show()
        
    except Exception as e:
        logger.error(f"Initialization failed: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    init_catalog()
