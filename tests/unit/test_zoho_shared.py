import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F
import sys
import os

# Add source path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../my_airflow/dags")))

class TestZohoSharedLogic:
    """Tests the shared extraction patterns used across Zoho Attendance, Leave, and Timesheet."""

    def test_zoho_response_nesting_extraction(self, spark):
        """Test extraction from multiple possible Zoho JSON response formats."""
        
        # 1. Format: _airbyte_data.response.result (Most common)
        raw_json_1 = '{"_airbyte_data": {"response": {"result": [{"id": 1, "val": "A"}]}}}'
        
        # 2. Format: response.result (Direct Zoho API or Airbyte flattened)
        raw_json_2 = '{"response": {"result": [{"id": 2, "val": "B"}]}}'
        
        # 3. Format: result (Old or custom)
        raw_json_3 = '{"result": [{"id": 3, "val": "C"}]}'
        
        data = [Row(raw_json=raw_json_1), Row(raw_json=raw_json_2), Row(raw_json=raw_json_3)]
        df = spark.createDataFrame(data)
        
        # Unified extraction logic used in Silver layers
        extraction_col = F.coalesce(
            F.get_json_object(F.col("raw_json"), "$._airbyte_data.response.result"),
            F.get_json_object(F.col("raw_json"), "$.response.result"),
            F.get_json_object(F.col("raw_json"), "$.result")
        )
        
        df_result = df.select(
            F.explode(
                F.from_json(extraction_col, "ARRAY<STRUCT<id:INT, val:STRING>>")
            ).alias("data")
        ).select("data.*")
        
        results = df_result.sort("id").collect()
        assert len(results) == 3
        assert results[0]["val"] == "A"
        assert results[1]["val"] == "B"
        assert results[2]["val"] == "C"

    def test_zoho_leave_days_map_parsing(self, spark):
        """Test the specific map parsing used in Leave transformation for the 'Days' field."""
        # Zoho leaves often return days as a MAP of date strings
        raw_leave_json = """
        {
            "EmployeeId": "EMP01",
            "Days": {
                "2026-02-23": {"LeaveCount": "1.0", "Session": "Full Day"},
                "2026-02-24": {"LeaveCount": "0.5", "Session": "Morning"}
            }
        }
        """
        data = [Row(record_json=raw_leave_json)]
        df = spark.createDataFrame(data)
        
        record_schema = "STRUCT<EmployeeId:STRING, Days:MAP<STRING, STRUCT<LeaveCount:STRING, Session:STRING>>>"
        
        df_parsed = df.withColumn("record", F.from_json(F.col("record_json"), record_schema))
        
        # Extract first day (logic used in leave_transformation.py)
        df_final = df_parsed.select(
            F.col("record.EmployeeId"),
            F.map_values(F.col("record.Days")).getItem(0).alias("first_day")
        )
        
        result = df_final.collect()[0]
        assert result.EmployeeId == "EMP01"
        assert result.first_day.LeaveCount == "1.0"
