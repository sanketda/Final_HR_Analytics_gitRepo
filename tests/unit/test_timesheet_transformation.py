import pytest
from pyspark.sql import Row, Window
from pyspark.sql import functions as F
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../my_airflow/dags")))

class TestTimesheetTransformation:
    """Standardized unit tests for Zoho Timesheet Transformation (Medallion Flow)."""

    @pytest.fixture
    def sample_raw_df(self, spark):
        """Mock Bronze layer raw data for Timesheet."""
        raw_json_1 = '{"response": {"result": ["{\\"timelogId\\": \\"101\\", \\"workDate\\": \\"23-Feb-2026\\", \\"hoursInMins\\": \\"480\\"}"]}}'
        data = [Row(raw_json=raw_json_1, ingest_timestamp="2026-02-23 09:00:00")]
        return spark.createDataFrame(data)

    def test_bronze_to_silver_timesheet_extraction(self, sample_raw_df):
        """Test extraction from Bronze to Silver for Timesheet Data."""
        df_exploded = sample_raw_df.select(
            F.col("ingest_timestamp"),
            F.explode(
                F.from_json(
                    F.get_json_object(F.col("raw_json"), "$.response.result"),
                    "ARRAY<STRING>"
                )
            ).alias("result_json")
        ).filter(F.col("result_json").isNotNull())
        
        df_flat = df_exploded.select(
            F.col("ingest_timestamp"),
            F.get_json_object(F.col("result_json"), "$.timelogId").alias("timelog_id"),
            F.get_json_object(F.col("result_json"), "$.hoursInMins").cast("int").alias("minutes_int")
        )
        
        results = df_flat.collect()
        assert len(results) == 1
        assert results[0]["timelog_id"] == "101"
        assert results[0]["minutes_int"] == 480

    def test_timesheet_deduplication(self, spark):
        """Test the windowing logic used for deduplication in Timesheet Silver layer."""
        data = [
            Row(timelog_id="T01", work_date="2026-02-23", minutes=480, ingest_timestamp="2026-02-23 09:00:00"),
            Row(timelog_id="T01", work_date="2026-02-23", minutes=510, ingest_timestamp="2026-02-23 10:00:00"), # Latest
            Row(timelog_id="T02", work_date="2026-02-23", minutes=450, ingest_timestamp="2026-02-23 09:30:00")
        ]
        df = spark.createDataFrame(data)
        
        window_spec = Window.partitionBy("timelog_id").orderBy(F.col("ingest_timestamp").desc())
        df_dedup = df.withColumn("row_num", F.row_number().over(window_spec)).filter("row_num = 1").drop("row_num")
        
        results = df_dedup.collect()
        assert len(results) == 2
        
        # Check that T01 kept the latest record (510 minutes)
        t01_record = [r for r in results if r.timelog_id == "T01"][0]
        assert t01_record.minutes == 510
