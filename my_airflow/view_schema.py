
from pyspark.sql import SparkSession
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_uc_metadata():
    packages = [
        "io.delta:delta-spark_2.12:3.0.0",
        "io.unitycatalog:unitycatalog-spark_2.12:0.2.1"
    ]
    
    # Try to connect to the UC server running in docker
    # Note: Use localhost since we are running this from the host machine
    spark = (SparkSession.builder
            .appName("CheckUCMetadata")
            .config("spark.jars.packages", ",".join(packages))
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.uc", "io.unitycatalog.spark.UCSingleCatalog")
            .config("spark.sql.catalog.uc.uri", "http://localhost:8801") # Assuming 8801 is mapped to 8080 or checking typical mapping
            .config("spark.sql.defaultCatalog", "uc")
            .getOrCreate())

    try:
        print("\n--- Catalogs ---")
        spark.sql("SHOW CATALOGS").show()
        
        print("\n--- Schemas in hr_analytics ---")
        spark.sql("USE CATALOG hr_analytics")
        spark.sql("SHOW SCHEMAS").show()
        
        for schema in ["bronze", "silver", "gold"]:
            print(f"\n--- Tables in hr_analytics.{schema} ---")
            spark.sql(f"SHOW TABLES IN hr_analytics.{schema}").show()

    except Exception as e:
        print(f"Error: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    check_uc_metadata()
