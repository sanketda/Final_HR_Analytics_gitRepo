import pytest
from fastapi.testclient import TestClient
from databricks_monitoring.webhook_diagnosis.webhook import app

client = TestClient(app)

def test_hello_endpoint():
    response = client.get("/hello")
    assert response.status_code == 200
    assert response.json()["message"] == "Webhook is running, listening for job notifs"

def test_notify_endpoint(mocker):
    mock_task = mocker.Mock()
    mock_task.id = "12345"
    mocker.patch("databricks_monitoring.webhook_diagnosis.webhook.run_diagnosis_task.delay", return_value=mock_task)

    response = client.post("/notify", json={"job": {"job_id": 1}, "run": {"run_id": 101}})
    assert response.status_code == 202
    assert response.json()["task_id"] == "12345"
