# Production-Ready ETL Testing Suite

This repository now includes an **Industry Standard** Data Engineering testing framework designed for PySpark and Airflow.

## Testing Layers Built

In a production environment, testing your ETL goes beyond unit testing Python functions. We have structured a layered testing framework:

### 1. Data Quality Testing (`tests/data_quality/`)
Ensures that data anomalies don't reach your business users.
- **Framework File:** `dq_framework.py` contains a `DataQualityChecker` class reminiscent of *Great Expectations* or *Deequ*.
- **Usage:** You can import this into your final DAG tasks (e.g. right before or after writing to `Gold` Delta Tables) and run assertions on the Delta dataframes for uniqueness, null-checks, and exact set matches.
- **Tests Example:** `test_data_quality.py` proves assertions locally against mock output schemas.

### 2. DAG Validation & Integrity Tests (`tests/dag_validation/`)
Ensures no bad configuration can crash your orchestration environment.
- **Tests for:**
  - Import errors / syntax crashes inside DAG files.
  - Cycle detection (checks that tasks don't loop indefinitely).
  - Validation of mandatory DAG policies like Retry Rules (`retries >= 1`).
- **How to Run:** Since this requires the Airflow library and Airflow Context, you should run this using Docker:
  ```bash
  sudo docker exec -it <airflow_webserver_container_name> pytest /opt/airflow/tests/dag_validation/
  ```

### 3. Pipeline Integration Testing (`tests/integration/`)
Validates that different modules work together correctly when moving data across endpoints.
- Simulates the execution of your pipeline reading from raw JSON, parsing with Spark, and writing into Delta tables using mock Temporary Directories (`tmpdir`).
- Makes sure Spark reads/writes match up with Transformation logic seamlessly without requiring access to Azure Blob Storage or Unity Catalog.

### 4. Unit Testing (`tests/unit/`)
Your existing tests for isolated transformations function perfectly here, asserting the micro-rules behind Time tracking and Zoho conversions.

## Master Orchestrator

To run the whole suite simultaneously, we created `tests/run_all_tests.sh`.

```bash
cd tests
chmod +x run_all_tests.sh
./run_all_tests.sh
```
*Note: Depending on your docker permissions, if `run_all_tests.sh` fails on the docker step, you can run the docker exec manually as well.*
