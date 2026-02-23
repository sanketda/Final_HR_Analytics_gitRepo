import os
import getpass
from typing import TypedDict, List, Dict, Any
from langgraph.graph import StateGraph, END
from langchain_groq import ChatGroq
from langchain_core.runnables import Runnable
from new_log_handler import log_handler_func
from send_mail import send_email
from log_fetcher import fetch_and_log_databricks_jobs
from rerun_cluster import start_cluster_run_job
from dotenv import load_dotenv
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import re
import json


from data_quality_agent import run_full_data_quality_pipeline


load_dotenv()  

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

EMAIL_CONFIG = {
    "sender_email": os.getenv("EMAIL_SENDER"),
    "app_password": os.getenv("EMAIL_PASSWORD"),
    "recipient_list": [os.getenv("EMAIL_RECIPIENT")],
    "smtp_server": os.getenv("SMTP_SERVER"),
    "smtp_port": int(os.getenv("SMTP_PORT"))
}

# Initialize Groq LLM
llm = ChatGroq(
    model="llama-3.1-8b-instant",
    temperature=0.1,
    max_tokens=None,
    timeout=None,
    max_retries=2,
)

# Define state schema using TypedDict
class GraphState(TypedDict):
    job_id: int
    directory_path: str
    results: List[Dict[str, Any]]
    data_quality_triggered: bool
    data_quality_results: List[Dict[str, Any]]
    email_sent: bool

def extract_error_groups(state):
    df = log_handler_func(state["directory_path"])
    print(df)

    # Extract and store the single job_id (assuming it's same for all rows)
    job_id = df["job_id"].iloc[0]
    state["job_id"] = job_id

    # Drop 'job_id' column as it's now redundant
    df = df.drop(columns=["job_id"])

    # Group by task_name and message (job_id removed)
    grouped = df.groupby(["task_name", "message"])

    results = []
    for (task_name, message), group in grouped:
        runs = group["runs"].unique().tolist()
        results.append({
            "runs": runs,
            "task_name": task_name,
            "message": message
        })

    state["results"] = results
    return state




def classify_error_types(state):
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

    for item in state["results"]:
        prompt = prompt_template.format(
            task_name=item["task_name"],
            message=item["message"]
        )
        response = llm.invoke(prompt)

        # Default
        error_type = "OTHER_ERROR"
        confidence = "Low"
        reason = "Could not parse"

        try:
            lines = response.content.strip().split("\n")
            for line in lines:
                if line.startswith("ERROR_TYPE:"):
                    error_type = line.split(":", 1)[1].strip()
                elif line.startswith("CONFIDENCE:"):
                    confidence = line.split(":", 1)[1].strip()
                elif line.startswith("REASON:"):
                    reason = line.split(":", 1)[1].strip()
        except Exception:
            pass

        item["error_type"] = error_type
        item["confidence"] = confidence
        item["reason"] = reason

    return state


def analyze_with_llm(state):
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

    for item in state["results"]:
        prompt = prompt_template.format(
            task_name=item["task_name"],
            message=item["message"],
            error_type=item["error_type"],
            confidence=item["confidence"],
            reason=item["reason"]
        )
        response = llm.invoke(prompt)

        # Defaults
        summary = ""
        root_cause = ""
        next_steps = ""
        severity = "Medium"
        action_owner = "Data Engineer"

        try:
            lines = response.content.strip().split("\n")
            for line in lines:
                if line.startswith("SUMMARY:"):
                    summary = line.split(":", 1)[1].strip()
                elif line.startswith("ROOT_CAUSE:"):
                    root_cause = line.split(":", 1)[1].strip()
                elif line.startswith("NEXT_STEPS:"):
                    next_steps = line.split(":", 1)[1].strip()
                elif line.startswith("SEVERITY:"):
                    severity = line.split(":", 1)[1].strip()
                elif line.startswith("ACTION_OWNER:"):
                    action_owner = line.split(":", 1)[1].strip()
        except Exception:
            pass

        item["diagnosis"] = f"Summary: {summary}\nRoot Cause: {root_cause}\nNext Steps: {next_steps}\nAction Owner: {action_owner}"
        item["severity_score"] = severity

        print(state["results"])

    return state




def trigger_cluster_terminated(state):
    """
    If CLUSTER_ERRORs exist, start the cluster and rerun the jobs.
    """
    # Check if cluster error exists
    if not any(err["error_type"] == "CLUSTER_ERROR" for err in state.get("error_types", [])):
        print("🟢 No cluster errors detected. Skipping cluster restart.")
        state["cluster_restart_triggered"] = False
        state["cluster_restart_results"] = []
        return state

   
    cluster_id = "0625-122045-rgpmcwe5"

    print(f"🔧 Cluster error detected! Restarting cluster '{cluster_id}' and rerunning jobs...")

    try:
        # Start the cluster and rerun jobs
        final_state = start_cluster_run_job(cluster_id)

        if final_state:
            state["cluster_restart_results"] = [{
                "cluster_id": cluster_id,
                "status": "SUCCESS",
                "jobs_rerun": final_state.get("jobs_rerun", []),
                "cluster_state": final_state.get("cluster_state", "Unknown")
            }]
        else:
            state["cluster_restart_results"] = [{
                "cluster_id": cluster_id,
                "status": "FAILED",
                "error": "Cluster operation returned None"
            }]
    except Exception as e:
        state["cluster_restart_results"] = [{
            "cluster_id": cluster_id,
            "status": "ERROR",
            "error": str(e)
        }]

    state["cluster_restart_triggered"] = True
    return state



def trigger_data_quality_pipeline(state):

    dbfs_path = "/FileStore/corrupted_scraped_data.csv"
    
    # Derive task name from path
    task = dbfs_path.strip("/").split("/")[-1]
    local_path = f"temp_{task}"

    # Check if data quality needs to be triggered
    if not any(err["error_type"] in ["TYPE_ERROR", "VALUE_ERROR"] for err in state.get("error_types", [])):
        print("📊 No data quality issues detected. Skipping data quality pipeline.")
        state["data_quality_triggered"] = False
        state["data_quality_results"] = []
        return state

    print("🔧 Data quality issues detected! Triggering data quality pipeline...")

    try:
        final_state = run_full_data_quality_pipeline(dbfs_path, local_path)

        if final_state:
            state["data_quality_results"] = [{
                "task": task,
                "status": "SUCCESS",
                "validation_result": final_state.get("validation", "No validation result"),
                "fixes_applied": len(final_state.get("fix_log", [])),
                "upload_status": final_state.get("upload_status", "Unknown")
            }]
        else:
            state["data_quality_results"] = [{
                "task": task,
                "status": "FAILED",
                "error": "Pipeline returned None"
            }]
    except Exception as e:
        state["data_quality_results"] = [{
            "task": task,
            "status": "ERROR",
            "error": str(e)
        }]

    state["data_quality_triggered"] = True
    return state




def send_email_notification(state):
    """Enhanced email notification with error summary and analysis"""
    try:
        email_subject = f"Pipeline Log Analysis Report - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

        job_id = state["job_id"]
        results = state.get("results", [])

        plain_body = f"Pipeline Log Analysis Report\n{'=' * 50}\n\n"
        html_body = """
        <html><head><style>
        body { font-family: Arial, sans-serif; background: #f4f4f4; padding: 20px; color: #333; }
        .container { background: #fff; padding: 30px; border-radius: 8px; max-width: 900px; margin: auto; box-shadow: 0 0 10px rgba(0,0,0,0.1); }
        .header { background: linear-gradient(135deg, #667eea, #764ba2); padding: 20px; border-radius: 8px; color: white; text-align: center; }
        .section-title { font-size: 18px; margin-top: 30px; font-weight: bold; }
        .error-group { border: 1px solid #ccc; border-radius: 6px; margin-top: 20px; padding: 15px; background: #fafafa; }
        .log-box { background: #eee; padding: 10px; border-radius: 6px; font-family: monospace; white-space: pre-wrap; }
        .badge { display: inline-block; padding: 4px 10px; background: #ff6b6b; color: white; border-radius: 20px; font-size: 12px; margin-left: 10px; }
        .summary-table { width: 100%; border-collapse: collapse; margin-top: 10px; }
        .summary-table th, .summary-table td { padding: 8px; border: 1px solid #ccc; text-align: left; }
        .diagnosis, .fix { background: #f0f0f0; padding: 10px; margin-top: 10px; border-radius: 6px; }
        </style></head><body><div class="container">
        <div class="header"><h2>🔍 Pipeline Log Analysis Report</h2><p>Automated System Monitoring with Error Classification</p></div>
        """

        html_body += f"""
        <p>Hello Team,</p>
        <p>Please review the detailed log analysis report below.<br>
        <strong>Pipeline Job ID:</strong> {job_id}</p>
        """

        # Build error summary
        error_summary = {}
        for result in results:
            err_type = result.get("error_type", "OTHER_ERROR")
            error_summary[err_type] = error_summary.get(err_type, 0) + 1

        if error_summary:
            html_body += "<div class='section-title'>Issue Summary by Type</div><table class='summary-table'><thead><tr><th>Error Type</th><th>Count</th></tr></thead><tbody>"
            for err_type, count in error_summary.items():
                html_body += f"<tr><td>{err_type}</td><td>{count}</td></tr>"
            html_body += "</tbody></table>"

        if not results:
            html_body += "<p>✅ No errors found. All systems are running smoothly!</p>"
            plain_body += "✅ No errors found.\n"
        else:
            plain_body += f"Job ID: {job_id}\n\nError Summary:\n"
            for etype, count in error_summary.items():
                plain_body += f"- {etype}: {count}\n"

            plain_body += "\nDetailed Errors:\n"

            for i, result in enumerate(results):
                runs = result.get("runs", [])
                task_name = result.get("task_name", "N/A")
                message = result.get("message", "")
                error_type = result.get("error_type", "UNKNOWN")
                severity = result.get("severity_score", "Medium")
                diagnosis = result.get("diagnosis", "No diagnosis available.")

                html_body += f"""
                <div class='error-group'>
                    <div><strong>⚠️ Error Group {i + 1}</strong><span class='badge'>{error_type}</span> <span class='badge'>{severity}</span></div>
                    <div class='section-title'>Task - {task_name}</div>
                    
                    <div class='section-title'>Runs</div>
                    <div>{', '.join(runs)}</div>
                    <div class='section-title'>Error Message</div>
                    <div class='log-box'>{message}</div>
                    <div class='section-title'>🔬 AI Diagnosis</div>
                    <div class='diagnosis'>{diagnosis}</div>
                </div>
                """

                plain_body += f"\nError Group {i + 1} [{error_type} - {severity}]\n"
                plain_body += f"Task: {task_name}\nRuns: {', '.join(runs)}\n"
                plain_body += f"Message: {message}\nDiagnosis: {diagnosis}\n"

        now_str = datetime.now().strftime('%Y-%m-%d at %H:%M:%S')
        html_body += f"""
        <p class='section-title'>📅 Report Generated</p>
        <p>{now_str}</p>
        <hr>
        <p style='font-size: 13px; color: gray;'>This is an automated email from Pipeline Monitor Bot</p>
        </div></body></html>
        """

        plain_body += f"\nReport generated on {now_str}\n\nRegards,\nPipeline Monitor Bot"


        success = send_email(
            subject=email_subject,
            plain_body=plain_body,
            html_body=html_body,
            email_config=EMAIL_CONFIG,
            recipients=EMAIL_CONFIG["recipient_list"]
        )

        state["email_sent"] = success


    except Exception as e:
        print(f"❌ Failed to send email: {str(e)}")
        state["email_sent"] = False

    return state




def fetch_logs_from_databricks(state: Dict[str, Any]) -> Dict[str, Any]:
    """LangGraph node to fetch Databricks job logs and store in directory."""
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")

    output_dir = state.get("directory_path", "log")
    fetch_and_log_databricks_jobs(host=host, token=token, log_dir=output_dir)

    state["directory_path"] = output_dir
    return state

def build_workflow():
    workflow = StateGraph(GraphState)

    # Add nodes
    workflow.add_node("fetch_logs", fetch_logs_from_databricks)
    workflow.add_node("parse_logs", extract_error_groups)
    workflow.add_node("classify_errors", classify_error_types)
    workflow.add_node("diagnose", analyze_with_llm)    
    workflow.add_node("trigger_data_quality", trigger_data_quality_pipeline)
    workflow.add_node("trigger_cluster", trigger_cluster_terminated)
    workflow.add_node("send_email", send_email_notification)

    # Entry point
    workflow.set_entry_point("fetch_logs")

    # Flow connections
    workflow.add_edge("fetch_logs", "parse_logs")
    workflow.add_edge("parse_logs", "classify_errors")
    workflow.add_edge("classify_errors", "diagnose")
    workflow.add_edge("diagnose", "send_email")
    # workflow.add_edge("trigger_data_quality", "trigger_cluster")
    # workflow.add_edge("trigger_cluster", "send_email")
    workflow.add_edge("send_email", END)

    return workflow.compile()


if __name__ == "__main__":
    print("🚀 Starting pipeline monitoring workflow...")

    # Initialize state
    initial_state = {
        "directory_path": "log",
        "results": [],
        "data_quality_triggered": False,
        "data_quality_results": [],
        "email_sent": False,
    }

    # Build and run the workflow
    graph = build_workflow()
    final_state = graph.invoke(initial_state)

    print("✅ Pipeline monitoring workflow completed.")
    print("📬 Email sent:", final_state.get("email_sent", False))