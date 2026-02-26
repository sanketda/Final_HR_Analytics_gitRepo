import pytest
from pyspark.sql import functions as F

class DataQualityChecker:
    """
    A lightweight, production-ready PySpark Data Quality Checker.
    Inspired by frameworks like Great Expectations and Deequ.
    """
    def __init__(self, df):
        self.df = df
        self.results = []
        self.passed = True

    def expect_column_to_exist(self, column):
        exists = column in self.df.columns
        self._record_result(f"Column '{column}' exists", exists)
        return self

    def expect_column_values_to_not_be_null(self, column):
        if column not in self.df.columns:
            self._record_result(f"Cannot check nulls for '{column}' - column missing", False)
            return self

        null_count = self.df.filter(F.col(column).isNull()).count()
        success = null_count == 0
        self._record_result(f"Column '{column}' is not null", success, f"Null count: {null_count}")
        return self

    def expect_column_values_to_be_unique(self, column):
        if column not in self.df.columns:
            self._record_result(f"Cannot check uniqueness for '{column}' - column missing", False)
            return self

        total_count = self.df.count()
        distinct_count = self.df.select(column).distinct().count()
        success = total_count == distinct_count
        self._record_result(f"Column '{column}' values are unique", success, 
                            f"Total: {total_count}, Distinct: {distinct_count}")
        return self

    def expect_column_values_to_be_in_set(self, column, value_set):
        if column not in self.df.columns:
            self._record_result(f"Cannot check values for '{column}' - column missing", False)
            return self

        invalid_count = self.df.filter(~F.col(column).isin(value_set)).count()
        success = invalid_count == 0
        self._record_result(f"Column '{column}' values in {value_set}", success, 
                            f"Invalid count: {invalid_count}")
        return self

    def expect_table_row_count_to_be_between(self, min_value, max_value):
        count = self.df.count()
        success = min_value <= count <= max_value
        self._record_result(f"Row count between {min_value} and {max_value}", success, f"Actual count: {count}")
        return self

    def _record_result(self, assertion_name, success, details=""):
        self.results.append({
            "assertion": assertion_name,
            "success": success,
            "details": details
        })
        if not success:
            self.passed = False

    def get_report(self):
        return {
            "passed": self.passed,
            "assertions": self.results
        }

    def assert_all(self):
        """Raises an exception if any assertion failed, summarizing the failures."""
        failures = [r for r in self.results if not r["success"]]
        if failures:
            error_msg = "Data Quality Checks Failed:\n"
            for f in failures:
                error_msg += f"- {f['assertion']} ({f['details']})\n"
            raise AssertionError(error_msg)
