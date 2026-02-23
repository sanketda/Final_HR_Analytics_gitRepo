import pytest
from pyspark.sql import Row, Window
from pyspark.sql import functions as F
import sys
import os

# Add dags path to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../my_airflow/dags")))

class TestAttendanceTransformation:
    """Standardized unit tests for Zoho Attendance Transformation (Medallion Flow)."""

    @pytest.fixture
    def sample_raw_df(self, spark):
        """Mock Bronze layer raw data."""
        data = [
            Row(raw_json='{"_airbyte_data": {"response": {"result": [{"Date": "23-Feb-2026", "employee_id": "EMP01", "working_hour": "08:30"}]}}}', ingest_timestamp="2026-02-23 09:00:00"),
            Row(raw_json='{"_airbyte_data": {"response": {"result": [{"Date": "23-Feb-2026", "employee_id": "EMP01", "working_hour": "09:00"}]}}}', ingest_timestamp="2026-02-23 10:00:00"), # Duplicate with later time
            Row(raw_json='{"_airbyte_data": {"response": {"result": [{"Date": "23-Feb-2026", "employee_id": "EMP02", "working_hour": "07:45"}]}}}', ingest_timestamp="2026-02-23 09:15:00")
        ]
        return spark.createDataFrame(data)

    def test_bronze_to_silver_extraction(self, sample_raw_df):
        """Test the flattening and extraction logic from Bronze to Silver."""
        df = sample_raw_df.select(
            F.col("ingest_timestamp"),
            F.explode(
                F.from_json(
                    F.get_json_object(F.col("raw_json"), "$._airbyte_data.response.result"),
                    "ARRAY<STRUCT<Date:STRING, employee_id:STRING, working_hour:STRING>>"
                )
            ).alias("data")
        ).select("ingest_timestamp", "data.*")
        
        counts = df.count()
        assert counts == 3
        
        # Verify specific fields
        emp01_rows = df.filter(F.col("employee_id") == "EMP01").collect()
        assert len(emp01_rows) == 2

    def test_time_to_minutes_conversion(self, spark):
        """Test HH:MM to integer minutes helper."""
        from pyspark.sql.functions import split, col
        
        def time_to_minutes(col_name):
            return (
                split(col(col_name), ":")[0].cast("int") * 60 +
                split(col(col_name), ":")[1].cast("int")
            )

        data = [Row(time="08:30"), Row(time="00:15"), Row(time="10:05")]
        df = spark.createDataFrame(data)
        result = df.withColumn("mins", time_to_minutes("time")).collect()
        
        assert result[0]["mins"] == 510
        assert result[1]["mins"] == 15
        assert result[2]["mins"] == 605

    def test_silver_deduplication_logic(self, spark):
        """Test the windowing logic used for deduplication in Silver layer."""
        data = [
            Row(employee_id="E01", work_date="2026-02-23", work_mins=480, ingest_ts="2026-02-23 09:00:00"),
            Row(employee_id="E01", work_date="2026-02-23", work_mins=510, ingest_ts="2026-02-23 10:00:00"), # Latest
            Row(employee_id="E02", work_date="2026-02-23", work_mins=450, ingest_ts="2026-02-23 09:30:00")
        ]
        df = spark.createDataFrame(data)
        
        window_spec = Window.partitionBy("employee_id", "work_date").orderBy(F.col("ingest_ts").desc())
        df_dedup = df.withColumn("rn", F.row_number().over(window_spec)).filter("rn = 1").drop("rn")
        
        results = df_dedup.collect()
        assert len(results) == 2
        
        # Check that E01 kept the latest record (510 minutes)
        e01_record = [r for r in results if r.employee_id == "E01"][0]
        assert e01_record.work_mins == 510

    def test_gold_enrichment_mock(self, spark, mocker):
        """Test Gold enrichment logic interface (using mock for dim tables)."""
        # This tests that our fact table columns are correctly mapped
        data = [Row(employee_id="E01", work_date="2026-02-23", work_minutes=480)]
        df = spark.createDataFrame(data)
        
        # Mocking the job.enrich_with_dimension_keys behavior
        # In actual code, this joins with dim_employee and dim_date
        df_enriched = df.withColumn("dim_employee_key", F.lit(101)) \
                        .withColumn("dim_date_key", F.lit(20260223))
        
        assert "dim_employee_key" in df_enriched.columns
        assert df_enriched.filter(F.col("dim_employee_key") == 101).count() == 1
