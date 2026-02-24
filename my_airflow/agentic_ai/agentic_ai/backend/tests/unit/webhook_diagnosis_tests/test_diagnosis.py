import json
import time
import pandas as pd
import pytest
from types import SimpleNamespace
import databricks_monitoring.webhook_diagnosis.diagnosis as diagnosis



class FakeResponse:
    def __init__(self, data):
        self._data = data

    def raise_for_status(self):
        return None

    def json(self):
        return self._data


class FakeSession:
    def __init__(self, responses):
        # responses: dict mapping endpoint identifier -> FakeResponse
        self.headers = {}
        self._responses = responses
        self.calls = []

    def get(self, url, params=None):
        self.calls.append((url, params))
        # choose response based on URL path
        if url.endswith("/api/2.2/jobs/runs/get"):
            return self._responses.get("run_get")
        elif url.endswith("/api/2.2/jobs/runs/get-output"):
            # match by run_id param if provided for more control
            return self._responses.get("get_output")
        else:
            # default response
            return FakeResponse({})


def make_llm_response(content_str):
    return SimpleNamespace(content=content_str)


def test_parse_notif_sets_run_and_job():
    notif = {"run": {"run_id": 123}, "job": {"job_id": 456}}
    state = {"job_notification_json": notif, "run_id": None, "job_id": None}
    out = diagnosis.parse_notif(state)
    assert out["run_id"] == 123
    assert out["job_id"] == 456


def test_fetch_runs_single_task(monkeypatch):
    # Prepare environment
    monkeypatch.setenv("DATABRICKS_HOST", "https://fake-host")
    monkeypatch.setenv("DATABRICKS_TOKEN", "fake-token")

    # Build fake run response with one task
    start_ms = 1_700_000_000_000
    end_ms = start_ms + 5000  # +5s
    run_data = {
        "tasks": [
            {
                "run_id": 999,
                "task_key": "my_task",
                "start_time": start_ms,
                "end_time": end_ms,
                "state": {
                    "result_state": "FAILED",
                    "life_cycle_state": "TERMINATED",
                    "user_cancelled_or_timedout": False,
                },
                "cluster_instance": {"cluster_id": "cls-1"},
                "notebook_task": {"notebook_path": "/Repos/notebook"},
            }
        ],
        "trigger": "scheduled",
        "run_type": "JOB",
        "run_page_url": "https://fake-host/runs/999",
    }
    get_output_data = {"error": "Example error message"}

    fake_responses = {
        "run_get": FakeResponse(run_data),
        "get_output": FakeResponse(get_output_data),
    }
    fake_session = FakeSession(fake_responses)

    # Patch requests.Session to return our fake session
    monkeypatch.setattr(diagnosis.requests, "Session", lambda: fake_session)

    state = {
        "run_id": 555,
        "job_id": 42,
        "job_notification_json": {},
        "logs_df": pd.DataFrame(),
    }
    out = diagnosis.fetch_runs(state)
    df = out["logs_df"]
    assert not df.empty
    assert list(df["task_name"])[0] == "my_task"
    # execution_duration in ms
    assert int(df["execution_duration"].iloc[0]) == end_ms - start_ms
    assert df["error_message"].iloc[0] == "Example error message"
    assert df["cluster_id"].iloc[0] == "cls-1"
    assert df["notebook_path"].iloc[0] == "/Repos/notebook"
    # run_page_url is from run_data
    assert df["run_page_url"].iloc[0] == "https://fake-host/runs/999"


def test_classify_logs_df_parses_llm_output(monkeypatch):
    # Create a logs_df with one row
    logs_df = pd.DataFrame(
        [
            {
                "task_run_id": 1,
                "task_name": "taskA",
                "error_message": "Something went wrong",
            }
        ]
    )
    state = {"logs_df": logs_df, "diagnosis_df": pd.DataFrame()}

    # Patch llm.invoke to return a formatted classification
    resp_text = "ERROR_TYPE: RESOURCE_ERROR\nCONFIDENCE: High\nREASON: Out of disk"
    monkeypatch.setattr(diagnosis, "llm", SimpleNamespace(invoke=lambda prompt: make_llm_response(resp_text)))

    out = diagnosis.classify_logs_df(state)
    diag = out["diagnosis_df"]
    assert not diag.empty
    assert diag.loc[0, "error_type"] == "RESOURCE_ERROR"
    assert diag.loc[0, "confidence"] == "High"
    assert "Out of disk" in diag.loc[0, "reason"]


def test_diagnose_logs_df_merges_and_parses(monkeypatch):
    # logs_df with details
    logs_df = pd.DataFrame(
        [
            {
                "task_run_id": 10,
                "task_name": "taskB",
                "error_message": "Connection refused",
                "job_id": 77,
                "run_id": 888,
            }
        ]
    )
    # diagnosis_df produced from classifier
    diagnosis_df = pd.DataFrame(
        [
            {
                "task_run_id": 10,
                "error_type": "NETWORK_ERROR",
                "confidence": "High",
                "reason": "Refused by remote host",
            }
        ]
    )
    state = {"logs_df": logs_df, "diagnosis_df": diagnosis_df}

    # Patch llm.invoke to return the expected diagnosis format
    resp_text = (
        "SUMMARY: Connection failed to external service\n"
        "ROOT_CAUSE: External service unreachable\n"
        "NEXT_STEPS: Check service endpoint and network\n"
        "SEVERITY: High\n"
        "ACTION_OWNER: DevOps\n"
    )
    monkeypatch.setattr(diagnosis, "llm", SimpleNamespace(invoke=lambda prompt: make_llm_response(resp_text)))

    out = diagnosis.diagnose_logs_df(state)
    diag = out["diagnosis_df"]
    # Should contain merged columns with diagnosis text and severity_score
    assert "diagnosis" in diag.columns
    assert "severity_score" in diag.columns
    row = diag[diag["task_run_id"] == 10].iloc[0]
    assert "Connection failed" in row["diagnosis"]
    assert row["severity_score"] == "High"
    assert row["job_id"] == 77 or pd.isna(row["job_id"]) is False  # keep job_id present


def test_send_email_notification_uses_send_email(monkeypatch):
    # Prepare simple merged data
    logs_df = pd.DataFrame(
        [
            {
                "task_run_id": 5,
                "task_name": "taskC",
                "error_message": "Fatal error",
            }
        ]
    )
    diagnosis_df = pd.DataFrame(
        [
            {
                "task_run_id": 5,
                "error_type": "TYPE_ERROR",
                "confidence": "Medium",
                "reason": "Invalid input",
                "diagnosis": "Summary: X\nRoot Cause: Y\nNext Steps: Z\nAction Owner: Team",
                "severity_score": "Medium",
            }
        ]
    )
    state = {"logs_df": logs_df, "diagnosis_df": diagnosis_df, "job_id": 999}

    sent = {}

    def fake_send_email(subject, plain_body, html_body, email_config, recipients):
        sent["subject"] = subject
        sent["body"] = plain_body
        sent["recipients"] = recipients
        return True

    monkeypatch.setattr(diagnosis, "send_email", fake_send_email)
    monkeypatch.setenv("EMAIL_RECIPIENT", "me@example.com")
    monkeypatch.setenv("EMAIL_SENDER", "from@example.com")
    monkeypatch.setenv("EMAIL_PASSWORD", "pw")
    monkeypatch.setenv("SMTP_SERVER", "smtp")
    monkeypatch.setenv("SMTP_PORT", "25")

    # Refresh module EMAIL_CONFIG to pick up env vars if needed
    # (diagnosis.EMAIL_CONFIG already created on import; but send_email_notification uses diagnosis.EMAIL_CONFIG)
    # So ensure recipients present
    diagnosis.EMAIL_CONFIG["recipient_list"] = ["me@example.com"]

    out = diagnosis.send_email_notification(state)
    assert out["email_sent"] is True
    assert "Pipeline Log Analysis Report - Job 999" in sent["subject"]
    assert "taskC" in sent["body"]
    assert sent["recipients"] == ["me@example.com"]