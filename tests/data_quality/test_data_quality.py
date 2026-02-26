import pytest
from pyspark.sql import Row
from dq_framework import DataQualityChecker

# In a real-world scenario, you would fetch these DataFrames directly from 
# the Gold/Silver Delta tables or MySQL after a pipeline run.
@pytest.fixture(scope="module")
def sample_gold_attendance(spark_session):
    """Mock final gold attendance data to test data quality assertions."""
    data = [
        Row(dim_employee_key=101, work_date="2026-02-23", hours_worked=8.0, is_present=True),
        Row(dim_employee_key=102, work_date="2026-02-23", hours_worked=7.5, is_present=True),
        Row(dim_employee_key=103, work_date="2026-02-23", hours_worked=0.0, is_present=False),
    ]
    return spark_session.createDataFrame(data)

def test_attendance_gold_data_quality(sample_gold_attendance):
    """Production-style data quality testing using the DQ framework."""
    checker = DataQualityChecker(sample_gold_attendance)
    
    # Assertions
    checker.expect_column_to_exist("dim_employee_key")
    checker.expect_column_to_exist("work_date")
    checker.expect_column_values_to_not_be_null("dim_employee_key")
    checker.expect_column_values_to_not_be_null("work_date")
    # Ensure all values are either True or False
    checker.expect_column_values_to_be_in_set("is_present", [True, False])
    
    # Will raise if any fail
    checker.assert_all()

@pytest.fixture(scope="module")
def spark_session():
    """Setup a lightweight local Spark session for tests."""
    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .appName("DQTestingSuite") \
        .config("spark.driver.memory", "512m") \
        .getOrCreate()
