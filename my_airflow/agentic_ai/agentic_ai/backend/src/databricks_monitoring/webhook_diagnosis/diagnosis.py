import os
import time
import json
import requests
import pandas as pd
from datetime import datetime
from typing import TypedDict

from dotenv import load_dotenv
from langgraph.graph.state import StateGraph
from langgraph.graph import END
from langchain_groq import ChatGroq
from send_mail import send_email
from database import save_df_to_postgres

load_dotenv()

# Email configuration
EMAIL_CONFIG = {
    "sender_email": os.getenv("EMAIL_SENDER"),
    "app_password": os.getenv("EMAIL_PASSWORD"),
    "recipient_list": [os.getenv("EMAIL_RECIPIENT")],
    "smtp_server": os.getenv("SMTP_SERVER"),
    "smtp_port": int(os.getenv("SMTP_PORT")),
}

# LLM setup
llm = ChatGroq(
    model="llama-3.1-8b-instant",
    temperature=0.1,
    max_tokens=None,
    timeout=None,
    max_retries=2,
)


# Graph state definition
class GraphState(TypedDict):
    run_id: int
    job_id: int
    job_notification_json: dict
    logs_df: pd.DataFrame
    diagnosis_df: pd.DataFrame
    email_sent: bool


# Step 1: Parse notification
def parse_notif(state: GraphState) -> GraphState:
    notif_data = state["job_notification_json"]
    state["run_id"] = notif_data.get("run", {}).get("run_id")
    state["job_id"] = notif_data.get("job", {}).get("job_id")
    return state


# Step 2: Fetch task run logs
def fetch_runs(state: GraphState) -> GraphState:
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})

    run_url = f"{host}/api/2.2/jobs/runs/get"
    run_params = {"run_id": state['run_id']}

    columns = [
        "task_run_id", "run_id", "job_id", "task_name", "start_time", "end_time",
        "execution_duration", "status", "error_message", "life_cycle_state",
        "result_state", "user_cancelled_or_timeout", "cluster_id", "trigger",
        "notebook_path", "run_type", "run_page_url"
    ]
    df = pd.DataFrame(columns=columns)

    try:
        run_response = session.get(run_url, params=run_params)
        run_response.raise_for_status()
        run_data = run_response.json()

        tasks = run_data.get("tasks", [])
        for task in tasks:
            task_run_id = task.get("run_id", "N/A")
            task_name = task.get("task_key", "N/A")
            start_time = task.get("start_time", 0)
            end_time = task.get("end_time", 0)

            task_data = {
                "task_run_id": task_run_id,
                "run_id": state['run_id'],
                "job_id": state['job_id'],
                "task_name": task_name,
                "start_time": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time / 1000)),
                "end_time": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time / 1000)),
                "execution_duration": (end_time - start_time) if start_time and end_time else 0,
                "status": task.get("state", {}).get("result_state", "N/A"),
                "error_message": session.get(
                    f"{host}/api/2.2/jobs/runs/get-output", params={"run_id": task_run_id}
                ).json().get("error", "No error"),
                "life_cycle_state": task.get("state", {}).get("life_cycle_state", "N/A"),
                "result_state": task.get("state", {}).get("result_state", "N/A"),
                "user_cancelled_or_timeout": task.get("state", {}).get("user_cancelled_or_timedout", "N/A"),
                "cluster_id": task.get("cluster_instance", {}).get("cluster_id", "N/A"),
                "trigger": run_data.get("trigger", "Unknown Trigger"),
                "notebook_path": task.get("notebook_task", {}).get("notebook_path", "N/A"),
                "run_type": run_data.get("run_type", "N/A"),
                "run_page_url": run_data.get("run_page_url", "N/A")
            }

            task_df = pd.DataFrame([task_data], columns=columns)
            df = pd.concat([df, task_df], ignore_index=True)

    except requests.exceptions.RequestException as e:
        print(f"Error fetching task details: {e}")
    except KeyError as e:
        print(f"Missing expected field in response: {e}")

    state['logs_df'] = df
    return state


# Step 3: Save logs
def save_logs(state: GraphState) -> GraphState:
    save_df_to_postgres(state['logs_df'], "task_runs")
    return state


# Step 4: Classify logs
def classify_logs_df(state: GraphState) -> GraphState:
    df = state["logs_df"]
    rows = []

    prompt_template = """You are an expert pipeline error classifier.

Error Details:
Task Name: {task_name}
Message: {message}

Classification Categories:
- TYPE_ERROR
- VALUE_ERROR
- RESOURCE_ERROR
- NETWORK_ERROR
- PERMISSION_ERROR
- CONFIGURATION_ERROR
- DEPENDENCY_ERROR
- CLUSTER_ERROR
- OTHER_ERROR
- NO_ERROR

Respond in this format:
ERROR_TYPE: [classification]
CONFIDENCE: [High/Medium/Low]
REASON: [Short reason]
"""

    for _, row in df.iterrows():
        prompt = prompt_template.format(
            task_name=row['task_name'], message=row['error_message']
        )
        response = llm.invoke(prompt)

        error_type, confidence, reason = "OTHER_ERROR", "Low", "Could not parse"
        try:
            lines = response.content.strip().split("\n")
            for line in lines:
                if line.startswith("ERROR_TYPE:"):
                    error_type = line.split(":", 1)[1].strip()
                elif line.startswith("CONFIDENCE:"):
                    confidence = line.split(":", 1)[1].strip()
                elif line.startswith("REASON:"):
                    reason = line.split(":", 1)[1].strip()
        except:
            pass

        rows.append({
            "task_run_id": row["task_run_id"],
            "error_type": error_type,
            "confidence": confidence,
            "reason": reason
        })

    state["diagnosis_df"] = pd.DataFrame(rows)
    return state


# Step 5: Generate diagnosis
def diagnose_logs_df(state: GraphState) -> GraphState:
    df = state["logs_df"]
    diag = state["diagnosis_df"]
    results = []

    prompt_template = """You are an AI assistant diagnosing pipeline errors.

Error Details:
Task Name: {task_name}
Message: {message}

Classification:
ERROR_TYPE: {error_type}
CONFIDENCE: {confidence}
REASON: {reason}

Please provide:
1. Short summary of the issue.
2. Likely root cause.
3. Recommended next steps.
4. Severity score (High / Medium / Low).
5. Who should take action.

Respond in this format:

SUMMARY: [text]
ROOT_CAUSE: [text]
NEXT_STEPS: [text]
SEVERITY: [High/Medium/Low]
ACTION_OWNER: [role]
"""

    merged = df.merge(diag, on="task_run_id", how="left")

    for _, row in merged.iterrows():
        prompt = prompt_template.format(
            task_name=row["task_name"],
            message=row["error_message"],
            error_type=row["error_type"],
            confidence=row["confidence"],
            reason=row["reason"]
        )
        response = llm.invoke(prompt)

        summary = root = next_steps = owner = ""
        severity = "Medium"

        try:
            lines = response.content.strip().split("\n")
            for line in lines:
                if line.startswith("SUMMARY:"):
                    summary = line.split(":", 1)[1].strip()
                elif line.startswith("ROOT_CAUSE:"):
                    root = line.split(":", 1)[1].strip()
                elif line.startswith("NEXT_STEPS:"):
                    next_steps = line.split(":", 1)[1].strip()
                elif line.startswith("SEVERITY:"):
                    severity = line.split(":", 1)[1].strip()
                elif line.startswith("ACTION_OWNER:"):
                    owner = line.split(":", 1)[1].strip()
        except:
            pass

        results.append({
    "task_run_id": row["task_run_id"],
    "diagnosis": f"Summary: {summary}\nRoot Cause: {root}\nNext Steps: {next_steps}\nAction Owner: {owner}",
    "severity_score": severity,
    "job_id": row.get("job_id"),
    "job_run_id": row.get("run_id")
})


    result_df = pd.DataFrame(results)
    state["diagnosis_df"] = diag.merge(result_df, on="task_run_id", how="left")
    return state


# Step 6: Save diagnosis to database
def save_diagnosis(state: GraphState) -> GraphState:
    save_df_to_postgres(state['diagnosis_df'], "diagnosis")
    return state


# Step 7: Send email
def send_email_notification(state: GraphState) -> GraphState:
    try:
        df = state["logs_df"]
        diagnosis_df = state["diagnosis_df"]
        job_id = state["job_id"]

        merged = df.merge(diagnosis_df, on="task_run_id", how="left")
        subject = f"Pipeline Log Analysis Report - Job {job_id}"

        plain_body = f"Pipeline Diagnosis Report for Job {job_id}\n\n"
        for _, row in merged.iterrows():
            plain_body += (
                f"\nTask: {row['task_name']} ({row['task_run_id']})\n"
                f"Error: {row['error_message']}\n"
                f"Type: {row['error_type']} ({row['severity_score']})\n"
                f"Diagnosis: {row['diagnosis']}\n"
                f"{'-'*40}\n"
            )
        plain_body += f"\nGenerated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"

        # HTML version omitted for brevity (same as your original)

        state["email_sent"] = send_email(
            subject=subject,
            plain_body=plain_body,
            html_body=None,  # optional, you can add your original HTML here
            email_config=EMAIL_CONFIG,
            recipients=EMAIL_CONFIG["recipient_list"]
        )

    except Exception as e:
        print("❌ Email send failed:", str(e))
        state["email_sent"] = False

    return state


# Graph Workflow Builder
def build_workflow():
    workflow = StateGraph(GraphState)
    workflow.add_node("parse notification", parse_notif)
    workflow.add_node("fetch and save logs", fetch_runs)
    workflow.add_node("save logs", save_logs)
    workflow.add_node("classify", classify_logs_df)
    workflow.add_node("diagnose", diagnose_logs_df)
    workflow.add_node("save diagnose", save_diagnosis)
    workflow.add_node("email", send_email_notification)

    workflow.set_entry_point("parse notification")
    workflow.add_edge("parse notification", "fetch and save logs")
    workflow.add_edge("fetch and save logs", "save logs")
    workflow.add_edge("save logs", "classify")
    workflow.add_edge("classify", "diagnose")
    workflow.add_edge("diagnose", "save diagnose")
    workflow.add_edge("save diagnose", "email")
    workflow.add_edge("email", END)

    return workflow.compile()


# Runner Function
def run_diagnosis(json_notification):
    initial_state: GraphState = {
        "run_id": None,
        "job_id": None,
        "job_notification_json": json_notification,
        "logs_df": pd.DataFrame(),
        "diagnosis_df": pd.DataFrame(),
        "email_sent": False
    }

    graph = build_workflow()
    final_state = graph.invoke(initial_state)

    print("✅ Done.")
    print("📬 Email sent:", final_state["email_sent"])


if __name__ == '__main__':
    with open("buffer.json", "r") as f:
        notif_data = json.load(f)

    run_diagnosis(notif_data)
