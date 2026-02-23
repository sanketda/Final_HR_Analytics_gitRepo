
import requests
import time
from utils.zoho_config import (
    AIRBYTE_SERVER, MAX_JOB_DURATION, MONITOR_INTERVAL #, AIRBYTE_PAT
)

# Use PAT directly
# def get_access_token():
#     return AIRBYTE_PAT
from utils.airbyte_pat_utils import get_airbyte_pat
AIRBYTE_PAT = get_airbyte_pat()


# Trigger connection sync
def trigger_connection_sync(access_token, connection_id):
    url = f"{AIRBYTE_SERVER}/api/public/v1/jobs"
    payload = {"connectionId": connection_id, "jobType": "sync"}
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    try:
        resp = requests.post(url, json=payload, headers=headers)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to trigger sync for {connection_id}:", e)
        return None

# Fetch job details
def fetch_job_details(access_token, job_id):
    url = f"{AIRBYTE_SERVER}/api/public/v1/jobs/{job_id}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    try:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to fetch job {job_id} details:", e)
        return None

# Cancel job
def cancel_job(access_token, job_id):
    url = f"{AIRBYTE_SERVER}/api/public/v1/jobs/{job_id}"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    try:
        resp = requests.delete(url, headers=headers)
        resp.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Failed to cancel job {job_id}:", e)
        return False

# Monitor job and cancel if needed
def monitor_and_cancel_job(access_token, job_id):
    start = time.time()
    while True:
        job = fetch_job_details(access_token, job_id)
        if not job:
            print(f"❌ Could not get job {job_id} details.")
            return
        status = job.get("status")
        duration = time.time() - start
        print(f"🕒 Job {job_id} | Status: {status} | Duration: {duration:.2f}s")

        if status in ["succeeded", "failed"]:
            print(f"✅ Job {job_id} ended with status: {status}")
            return
        if duration > MAX_JOB_DURATION:
            print(f"⏱ Job {job_id} exceeded {MAX_JOB_DURATION}s. Cancelling...")
            cancel_job(access_token, job_id)
            return
        time.sleep(MONITOR_INTERVAL)

# Main function
def run_connection_trigger(connection_id):
    try:
        print(f"🔹 Triggering Airbyte sync for Connection ID: {connection_id}")
        access_token = AIRBYTE_PAT #get_access_token()
        sync = trigger_connection_sync(access_token, connection_id)
        if not sync:
            return False
        job_id = sync.get("jobId")
        print(f"🚀 Sync triggered! Job ID: {job_id}")
        monitor_and_cancel_job(access_token, job_id)
    except Exception as e:
        print(f"⚠️ Error while triggering connection {connection_id}: {e}")
        return False


         
