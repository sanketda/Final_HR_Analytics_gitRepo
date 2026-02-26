import pytest
from pyspark.sql import Row, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../my_airflow/dags")))

class TestEmployeeTransformation:
    """Standardized unit tests for Employee Transformation & SCD2 Injection."""

    @pytest.fixture
    def sample_raw_df(self, spark):
        """Mock Bronze layer raw data for Employee mapped dictionary."""
        raw_json_1 = '{"response": {"result": {"1": {"EmployeeID": "EMP01", "FirstName": "John", "LastName": "Doe", "EmailID": "john@test.com"}}}}'
        data = [Row(raw_json=raw_json_1, ingest_timestamp="2026-02-23 09:00:00")]
        return spark.createDataFrame(data)

    def test_bronze_to_silver_employee_map_extraction(self, sample_raw_df):
        """Test extraction of Employees when they come in as mapped dictionary."""
        payload_col = F.get_json_object(F.col("raw_json"), "$.response.result")
        emp_struct_schema = "STRUCT<EmployeeID:STRING, FirstName:STRING, LastName:STRING>"
        
        df_map = sample_raw_df.withColumn("payload", payload_col) \
            .select(
                F.col("ingest_timestamp"),
                F.explode(F.from_json(F.col("payload"), "MAP<STRING, STRING>")).alias("zoho_record_id", "record_json")
            ).select(
                F.col("ingest_timestamp"),
                F.from_json(F.col("record_json"), emp_struct_schema).alias("employee")
            )
            
        df_flat = df_map.select(
            F.col("employee.EmployeeID").alias("employee_id"),
            F.col("employee.FirstName").alias("first_name"),
            F.col("employee.LastName").alias("last_name")
        )
        
        # Test derived column simulation (e.g. Full Name calculation)
        df_derived = df_flat.withColumn("full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
        
        results = df_derived.collect()
        assert len(results) == 1
        assert results[0].employee_id == "EMP01"
        assert results[0].full_name == "John Doe"

    def test_scd2_hash_generation(self, spark):
        """Test the hashing function logic for CDC checks."""
        data = [
            Row(employee_id="EMP01", first_name="John", last_name="Doe", department="Engineering", ingest_timestamp="2026-02-23 09:00:00")
        ]
        df = spark.createDataFrame(data)
        
        scd_cols_to_hash = ["first_name", "last_name", "department"]
        df_with_hash = df.withColumn(
            "record_hash",
            F.sha2(F.concat_ws("||", *scd_cols_to_hash), 256)
        )
        
        results = df_with_hash.collect()
        assert results[0].record_hash is not None
