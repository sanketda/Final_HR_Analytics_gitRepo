import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """Setting up a local Spark session for testing."""
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("unittest-spark-session")
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
