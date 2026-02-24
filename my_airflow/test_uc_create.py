
from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run():
    spark = (SparkSession.builder
            .appName("TestTableCreation")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.uc", "io.unitycatalog.spark.UCSingleCatalog")
            .config("spark.sql.catalog.uc.uri", "http://unity-catalog:8080")
            .config("spark.sql.defaultCatalog", "uc")
            .getOrCreate())

    try:
        logger.info("Current Catalog: " + spark.catalog.currentCatalog())
        logger.info("Current Database: " + spark.catalog.currentDatabase())
        
        # Try to show databases in 'uc'
        spark.sql("SHOW SCHEMAS").show()
        
        # Try to create a table in gold
        logger.info("Creating test table...")
        spark.sql("CREATE TABLE IF NOT EXISTS gold.test_connection (id INT) USING DELTA")
        logger.info("Table created successfully!")
        
        spark.sql("SHOW TABLES IN gold").show()
    except Exception as e:
        logger.error(f"Failed: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    run()
