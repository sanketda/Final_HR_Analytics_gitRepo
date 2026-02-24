
import requests
import logging
from datetime import datetime

# Teams Webhook URL provided by user
TEAMS_WEBHOOK_URL = "https://shyenatechyarns1.webhook.office.com/webhookb2/f3ff73f0-1d49-4906-9230-480cf9d2c231@db89c5d1-2290-4be1-b06d-4c2c6df1c8f4/IncomingWebhook/3ec9e4e39e1b43eaaaa15dd857dd2284/cb2bdc8d-e46b-4f13-aef9-43fd927e5640/V2nmHKir9n6UtFhU4no0RwVSvoAdokg2usdK9dHz1H5s01"

logger = logging.getLogger(__name__)

def send_teams_alert(**context):
    """
    Sends a notification card to Microsoft Teams.
    Designed to be used with Airflow PythonOperator with provide_context=True.
    """
    try:
        dag_id = context.get('dag').dag_id
        task_id = context.get('task_instance').task_id
        execution_date = context.get('execution_date')
        state = context.get('task_instance').state
        exception = context.get('exception')
        
        # Determine Status
        # If this function is called as a success callback or downstream of successful tasks
        if state == 'success' or state is None: # state might be None if running currently
             # In a PythonOperator, if we are inside the callable, the task is 'running'.
             # Usually this is used as a specific task at the end of the pipeline.
             status = "SUCCESS"
             theme_color = "00FF00" # Green
             summary = f"Pipeline Succeeded: {dag_id}"
        else:
             status = "FAILURE"
             theme_color = "FF0000" # Red
             summary = f"Pipeline Failed: {dag_id}"

        # If we are explicitly calling this for failure (e.g. from on_failure_callback), override status
        if exception:
            status = "FAILURE"
            theme_color = "FF0000"
            summary = f"Pipeline Failed: {dag_id}"

        # Build Card logic
        # If called as a separate task at the end (standard pattern in valid DAG), we treat it as Success Notification.
        # If called via callback, it adapts.
        
        # We'll assume usage as a Task for Success, and potentially a Task or Callback for Failure.
        # Let's look at the DAG usage: currently replacing email_success.
        
        # Setup specific messaging
        if "success" in task_id.lower():
            status = "SUCCESS"
            theme_color = "00FF00"
            message_text = "All Zoho People attendance and leave data tasks finished successfully. 🚀"
        elif "fail" in task_id.lower():
            status = "FAILURE"
            theme_color = "FF0000"
            message_text = "The pipeline encountered an error. Please check logs."
        else:
            # Default fallback
            message_text = f"Task {task_id} completed."

        card = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": theme_color,
            "summary": summary,
            "sections": [{
                "activityTitle": f"📢 Zoho Pipeline: {status}",
                "activitySubtitle": f"DAG: {dag_id}",
                "activityImage": "https://upload.wikimedia.org/wikipedia/commons/thumb/c/c9/Microsoft_Office_Teams_%282018%E2%80%93present%29.svg/1200px-Microsoft_Office_Teams_%282018%E2%80%93present%29.svg.png",
                "facts": [
                    {"name": "Execution Date", "value": str(execution_date)},
                    {"name": "Timestamp", "value": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
                    {"name": "Status", "value": status}
                ],
                "markdown": True,
                "text": f"**Details:**\n{message_text}"
            }],
            "potentialAction": [{
                "@type": "OpenUri",
                "name": "View in Airflow",
                "targets": [{"os": "default", "uri": "http://164.52.195.88:8080/"}] # Assuming standard Airflow URL or the IP found in code
            }]
        }

        response = requests.post(TEAMS_WEBHOOK_URL, json=card)
        response.raise_for_status()
        logger.info(f"Teams notification sent. Status Code: {response.status_code}")
        
    except Exception as e:
        logger.error(f"Failed to send Teams notification: {e}")
