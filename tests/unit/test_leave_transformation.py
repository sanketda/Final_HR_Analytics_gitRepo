import pytest
from pyspark.sql import Row, Window
from pyspark.sql import functions as F
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../my_airflow/dags")))

class TestLeaveTransformation:
    """Standardized unit tests for Zoho Leave Transformation (Medallion Flow)."""

    @pytest.fixture
    def sample_raw_df(self, spark):
        """Mock Bronze layer raw data for Leave."""
        raw_json_1 = '''
        {
            "response": {
                "result": {
                    "record_1": {
                        "EmployeeId": "EMP01",
                        "Days": {
                            "2026-02-23": {"StartTime": "09:00", "EndTime": "18:00", "LeaveCount": "1.0", "Session": "Full Day"}
                        },
                        "From": "23-Feb-2026"
                    }
                }
            }
        }
        '''
        data = [Row(raw_json=raw_json_1, ingest_timestamp="2026-02-23 09:00:00")]
        return spark.createDataFrame(data)

    def test_bronze_to_silver_leave_extraction(self, sample_raw_df):
        """Test extraction from Bronze to Silver for Leave Data, particularly the nested Maps."""
        df_flat_raw = sample_raw_df.select(
            F.col("ingest_timestamp"),
            F.explode(
                F.from_json(
                    F.get_json_object(F.col("raw_json"), "$.response.result"),
                    "MAP<STRING, STRING>"
                )
            ).alias("zoho_record_id", "record_json")
        )
        
        record_schema = "STRUCT<EmployeeId:STRING, `From`:STRING, Days:MAP<STRING, STRUCT<StartTime:STRING, EndTime:STRING, LeaveCount:STRING, Session:STRING>>>"
        
        df_flat = df_flat_raw.withColumn("record", F.from_json(F.col("record_json"), record_schema))
        df_cleaned = df_flat.select(
            F.col("ingest_timestamp"),
            F.col("record.EmployeeId").alias("employee_id"),
            F.map_values(F.col("record.Days")).getItem(0).alias("first_day")
        ).select(
            "employee_id",
            F.col("first_day.LeaveCount").alias("leave_count")
        )

        results = df_cleaned.collect()
        assert len(results) == 1
        assert results[0]["employee_id"] == "EMP01"
        assert results[0]["leave_count"] == "1.0"
