import pytest
from databricks_monitoring.webhook_diagnosis.tasks import run_diagnosis_task

def test_run_diagnosis_task(mocker):
    mock_run = mocker.patch("databricks_monitoring.webhook_diagnosis.tasks.run_diagnosis")
    payload = {"job": {"job_id": 123}, "run": {"run_id": 456}}

    run_diagnosis_task.run(payload)   # direct call instead of Celery queue

    mock_run.assert_called_once_with(payload)
