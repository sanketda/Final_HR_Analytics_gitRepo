import os
# Secrets removed to comply with security rules.
# Ensure these environment variables are set in your execution environment.
# os.environ["DATABRICKS_HOST"] = os.getenv("DATABRICKS_HOST", "REPLACED_SECRET")
# os.environ["DATABRICKS_TOKEN"] = os.getenv("DATABRICKS_TOKEN", "REPLACED_SECRET")
# os.environ["GROQ_API_KEY"] = os.getenv("GROQ_API_KEY", "REPLACED_SECRET")

import time
import requests

def start_cluster_run_job(cluster_id):
    """
    Starts a Databricks cluster and then triggers a job.

    Args:
        cluster_id (str): The ID of the cluster to start

    Returns:
        bool: True if everything succeeded, False otherwise
    """
    job_id = 481014901258292
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    headers = {
        "Authorization": f"Bearer {token}"
    }

    try:
        # 1️⃣ Start the cluster
        print(f"🔄 Starting cluster {cluster_id}...")
        start_resp = requests.post(
            f"{host}/api/2.0/clusters/start",
            headers=headers,
            json={"cluster_id": cluster_id}
        )
        if start_resp.status_code != 200:
            print(f"❌ Failed to start cluster: {start_resp.text}")
            return False
        else:
            print("✅ Cluster start initiated successfully.")

        # 2️⃣ Poll cluster status until RUNNING
        while True:
            get_resp = requests.get(
                f"{host}/api/2.0/clusters/get",
                headers=headers,
                params={"cluster_id": cluster_id}
            )
            if get_resp.status_code != 200:
                print(f"❌ Failed to get cluster status: {get_resp.text}")
                return False

            state = get_resp.json().get("state")
            print(f"⏳ Cluster state: {state}")

            if state == "RUNNING":
                print("✅ Cluster is running.")
                break
            elif state in ("TERMINATED", "ERROR", "UNKNOWN"):
                print(f"❌ Cluster entered unexpected state: {state}")
                return False
            else:
                time.sleep(10)  # Wait and poll again

        # 3️⃣ Run the job
        print(f"🚀 Starting job {job_id}...")
        run_resp = requests.post(
            f"{host}/api/2.1/jobs/run-now",
            headers=headers,
            json={"job_id": job_id}
        )
        if run_resp.status_code != 200:
            print(f"❌ Failed to run job: {run_resp.text}")
            return False

        run_data = run_resp.json()
        run_id = run_data.get("run_id")
        print(f"✅ Job started successfully (Run ID: {run_id})")

        return True

    except Exception as e:
        print(f"❌ Exception occurred: {str(e)}")
        return False


# if __name__ == "__main__":
#     # Load from environment
#     # DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
#     # DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

#     # # Replace these with your IDs
#     # cluster_id = "0625-122045-rgpmcwe5"
#     # job_id = 481014901258292

#     # result = start_cluster_and_run_job(
    
#     #     cluster_id=cluster_id,
        
#     # )
#     # print("🎉 Job Run Response:", result)
