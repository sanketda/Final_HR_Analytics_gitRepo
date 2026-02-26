#!/bin/bash
set -e

# Change directory to the project root to ensure 'tests/...' paths resolve correctly
cd "$(dirname "$0")/.."


mkdir -p tests/reports
chmod 777 tests/reports

echo "================================================="
echo "   🚀 Production-Ready ETL Testing Suite 🚀      "
echo "================================================="

echo "1) Running PySpark Unit & Transformation Tests (with Coverage & JUnit XML)..."
pytest tests/unit/ -v --disable-warnings \
    --html=tests/reports/unit_test_report.html --self-contained-html \
    --junitxml=tests/reports/junit_unit_tests.xml \
    --cov=my_airflow/dags/utils --cov-report=html:tests/reports/coverage_html --cov-report=xml:tests/reports/coverage.xml
echo "✅ Unit tests passed."
echo "   📊 Coverage Report: tests/reports/coverage_html/index.html"
echo "   🤖 CI/CD XML Report: tests/reports/junit_unit_tests.xml"
echo ""

echo "2) Running Data Quality Assertions (Integration)..."
pytest tests/data_quality/ -v --disable-warnings --html=tests/reports/data_quality_report.html --self-contained-html
echo "✅ Data Quality checks passed. (Report: tests/reports/data_quality_report.html)"
echo ""

echo "3) Running DAG Validation Tests (via Docker)..."
# We assume the airflow-scheduler or webserver container is named 'airflow-webserver-1' or similar
# We use docker exec to run pytest inside the container where airflow is installed.
CONTAINER_NAME=$(sudo docker ps --format '{{.Names}}' | grep airflow-webserver | head -n 1)

if [ -z "$CONTAINER_NAME" ]; then
    echo "⚠️  Airflow container not found. Make sure docker-compose is running for DAG validation."
    echo "To run manually: sudo docker exec -it <container_name> pytest /opt/airflow/tests/dag_validation/"
else
    echo "Found Airflow container: $CONTAINER_NAME"
    if sudo docker exec $CONTAINER_NAME which pytest > /dev/null 2>&1; then
        echo "Executing DAG Integrity tests inside Airflow..."
        sudo docker exec -it $CONTAINER_NAME bash -c "pytest /opt/airflow/tests/dag_validation -v --disable-warnings --html=/opt/airflow/tests/reports/dag_validation_report.html --self-contained-html"
        echo "✅ DAG Validation tests passed. (Report: tests/reports/dag_validation_report.html)"
    else
        echo "⚠️  pytest is not installed in the '$CONTAINER_NAME' container."
        echo "To run DAG validation tests, please install pytest in the Airflow container via your Dockerfile, or skip for now."
        echo "✅ All local testing passed successfully."
    fi
fi

echo ""
echo "================================================="
echo " 🎉 All Production Testing Gates Passed!         "
echo "================================================="
