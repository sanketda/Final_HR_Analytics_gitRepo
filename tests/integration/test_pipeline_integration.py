import os
import pytest
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def test_full_pipeline_transformation_logic(spark_session, tmpdir):
    """
    Production-ready integration test simulating an End-To-End Spark transformation job.
    This creates mock raw data on disk, reads it, processes it, and writes the output.
    """
    # 1. Setup Mock "Bronze" Data in a temporary directory
    bronze_dir = os.path.join(tmpdir, "bronze")
    os.makedirs(bronze_dir, exist_ok=True)
    
    mock_data = [
        Row(dim_employee_key=101, work_date="2026-02-23", hours="8"),
        Row(dim_employee_key=102, work_date="2026-02-23", hours="9"),
        Row(dim_employee_key=101, work_date="2026-02-24", hours="7")
    ]
    df_raw = spark_session.createDataFrame(mock_data)
    
    # Save as JSON to simulate raw storage
    json_path = os.path.join(bronze_dir, "raw_data.json")
    df_raw.write.json(json_path)
    
    # 2. Simulate ETL Read
    df_read = spark_session.read.json(json_path)
    
    # 3. Simulate ETL Transform (Silver -> Gold)
    from pyspark.sql import functions as F
    df_transformed = df_read.withColumn("hours_worked", F.col("hours").cast("int")) \
                            .withColumn("is_overtime", F.col("hours_worked") > 8) \
                            .drop("hours")
                            
    # 4. Simulate ETL Write
    gold_dir = os.path.join(tmpdir, "gold")
    df_transformed.write.parquet(gold_dir)
    
    # 5. Assert Integration Outcomes
    df_final = spark_session.read.parquet(gold_dir)
    results = df_final.collect()
    
    assert len(results) == 3
    
    # Check proper transformation logic
    overtime_record = [r for r in results if r.dim_employee_key == 102][0]
    assert overtime_record.hours_worked == 9
    assert overtime_record.is_overtime is True

@pytest.fixture(scope="module")
def spark_session():
    """Setup a lightweight local Spark session for tests."""
    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .appName("IntegrationTestingSuite") \
        .config("spark.driver.memory", "512m") \
        .getOrCreate()
