import json
import os
from dotenv import load_dotenv
import requests
import pandas as pd


def get_jobs():
    jobs_data = []
    headers = {
        "Authorization": f"Bearer {token}"
    }

    url = f"{host}/api/2.2/jobs/list?expand_jobs=true&limit=100"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        jobs = response.json().get("jobs", [])

        for job in jobs:
            job_metadata = {
                "job_id": job.get("job_id"),
                "name": job.get("settings", {}).get("name"),
                "creator": job.get("creator_user_name"),
                "created_time": job.get("created_time"),


                "description": job.get("settings", {}).get("description"),
                "email_notifications_on_failure": job.get("settings", {}).get("email_notifications", {}).get("on_failure"),
                
                "schedule_timezone": job.get("settings", {}).get("schedule", {}).get("timezone_id"),
                "state": job.get("state", {}).get("life_cycle_state"),
                "cluster_size": job.get("settings", {}).get("job_clusters", [{}])[0].get("new_cluster", {}).get("autoscale", {}).get("max_workers"),
                


                "webhook_notifications_on_failure": job.get("settings", {}).get("webhook_notifications", {}).get("on_failure"),
                "webhook_notifications_on_success": job.get("settings", {}).get("webhook_notifications", {}).get("on_success"),
                "webhook_notifications_on_start": job.get("settings", {}).get("webhook_notifications", {}).get("on_start"),
                "webhook_notifications_on_duration_warning_threshold_exceeded": job.get("settings", {}).get("webhook_notifications", {}).get("on_duration_warning_threshold_exceeded"),
                "webhook_notifications_on_streaming_backlog_exceeded": job.get("settings", {}).get("webhook_notifications", {}).get("on_streaming_backlog_exceeded"),
            }
            jobs_data.append(job_metadata)
    else:
        print(f"Error fetching jobs: {response.status_code}")

    return jobs_data


if __name__ == '__main__':

    load_dotenv()
    host = os.getenv("DATABRICKS_HOST")

    token = os.getenv("DATABRICKS_TOKEN")

    jobs = get_jobs()
    df_jobs = pd.DataFrame(jobs)
    df_jobs.to_csv("job_details.csv", index=False)

    # with open('databricks_jobs_metadata.json', 'w') as json_file:
    #     json.dump(jobs, json_file, indent=4)

    print(df_jobs)
