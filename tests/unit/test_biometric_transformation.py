import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import sys
import os
import re

# Add source path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../my_airflow/dags")))

class TestBiometricTransformation:
    """Standardized unit tests for Biometric (Excel) Transformation."""

    def test_excel_column_name_cleaning(self):
        """Test the logic that converts Excel headers to Spark-friendly column names."""
        def clean_col(c: str) -> str:
            c = re.sub(r"[.\s]+", "_", c.strip())
            return re.sub(r"_+", "_", c).strip("_")

        assert clean_col("Employee ID") == "Employee_ID"
        assert clean_col("Work.Duration ") == "Work_Duration"
        assert clean_col("  Time...In  ") == "Time_In"
        assert clean_col("Normal-Time") == "Normal-Time" # re.sub only handles dots and spaces
        assert clean_col("First Name (Legal)") == "First_Name_(Legal)" # Spaces replaced

    def test_biometric_date_parsing_from_metadata(self):
        """Test extraction of 'Attendance Date' from raw Excel text dump."""
        metadata_text = """
        Company Name: ABC Tech
        Attendance Report
        Attendance Date : 23-Feb-2026
        Location: Head Office
        """
        m = re.search(r"Attendance Date\s*[:\-]\s*(\d{2}-\w{3}-\d{4})", metadata_text)
        work_date_str = m.group(1) if m else None
        
        assert work_date_str == "23-Feb-2026"

    def test_biometric_time_to_minutes(self, spark):
        """Test HH:MM to integer minutes helper."""
        from pyspark.sql.functions import col, split
        
        def time_to_seconds(col_name):
            return (
                split(col(col_name), ":")[0].cast("int") * 60 +
                split(col(col_name), ":")[1].cast("int")
            )

        data = [Row(dur="08:30"), Row(dur="01:05")]
        df = spark.createDataFrame(data)
        result = df.withColumn("mins", time_to_seconds("dur")).collect()
        
        assert result[0]["mins"] == 510
        assert result[1]["mins"] == 65

    def test_silver_column_mapping_and_casting(self, spark):
        """Test mapping and casting logic used in Silver layer."""
        # Create a mock Bronze DataFrame with varied column names (case-insensitive simulation)
        data = [Row(E_Code=101, Shift="General", A_InTime="09:00", Work_Dur="08:00", Status="P", ingest_work_date="2026-02-23")]
        df_bronze = spark.createDataFrame(data)
        
        # Logic from Silver: find e_code/ecode
        employee_id_col = F.coalesce(
            *[F.col(c) for c in df_bronze.columns if c.lower() in ["e_code", "ecode"]]
        ).cast(IntegerType())
        
        df_silver = df_bronze.select(
            employee_id_col.alias("employee_id"),
            F.col("Shift").alias("shift"),
            F.to_date(F.col("ingest_work_date")).alias("work_date")
        )
        
        result = df_silver.collect()[0]
        assert result["employee_id"] == 101
        assert str(result["work_date"]) == "2026-02-23"
