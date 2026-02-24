import os
import time
import requests
import logging
from datetime import datetime
from pathlib import Path
import re

def clean_filename(name):
    return re.sub(r'[^\w\d\-_.]', '_', name)

def fetch_and_log_databricks_jobs(host: str, token: str, log_dir: str = "log"):
    if not host or not token:
        print("❌ DATABRICKS_HOST or DATABRICKS_TOKEN not provided.")
        return

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})

    log_root_dir = Path(log_dir)
    log_root_dir.mkdir(exist_ok=True)

    # ========== Fetch All Jobs ==========
    try:
        print("📦 Fetching all jobs...")
        resp = session.get(f"{host}/api/2.1/jobs/list")
        resp.raise_for_status()
        jobs = resp.json().get("jobs", [])
    except Exception as e:
        print(f"❌ Error fetching job list: {e}")
        return

    if not jobs:
        print("❌ No jobs found.")
        return

    print(f"✅ Found {len(jobs)} jobs. Sorting by most recent execution...\n")

    # ========== Attach Latest Run Timestamps to Each Job ==========
    job_runs_info = []
    for job in jobs:
        job_id = job["job_id"]
        try:
            params = {"job_id": job_id, "limit": 1, "active_only": False}
            run_resp = session.get(f"{host}/api/2.1/jobs/runs/list", params=params)
            run_resp.raise_for_status()
            runs = run_resp.json().get("runs", [])
            start_time = runs[0].get("start_time", 0) if runs else 0
        except Exception:
            start_time = 0

        job_runs_info.append((start_time, job))

    sorted_jobs = sorted(job_runs_info, key=lambda x: x[0])
    print("📄 Recently Run Jobs:\n")
    for idx, (last_run_ts, job) in enumerate(sorted_jobs, 1):
        job_name = job["settings"].get("name", f"Job_{job['job_id']}")
        job_id = job["job_id"]
        if last_run_ts == 0:
            readable_time = "Never Run"
        else:
            readable_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_run_ts / 1000))
        print(f"{idx:2d}. 🧪 Job '{job_name}' (ID: {job_id}) — Last Run: {readable_time}")
    print()
    # ========== Process Each Job in Order ==========
    for start_time, job in sorted_jobs:
        job_id = job["job_id"]
        job_name = job["settings"].get("name", f"Job_{job_id}")
        cleaned_name = clean_filename(job_name)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_path = log_root_dir / f"{cleaned_name}_{job_id}_{timestamp}.log"

        logging.basicConfig(
            filename=log_path,
            level=logging.INFO,
            format="[%(asctime)s] %(group_id)s - %(levelname)s - %(message)s",
            style='%',
            force=True
        )

        logger = logging.getLogger()
        logger.info(f"{cleaned_name} - 🆕 Logging for job: {job_name} (ID: {job_id})", extra={"group_id": 0})
        print(f"🆕 Job: {job_name} (ID: {job_id}) → log saved to: {log_path.name}")

        try:
            params = {"job_id": job_id, "limit": 5, "active_only": False}
            run_resp = session.get(f"{host}/api/2.1/jobs/runs/list", params=params)
            run_resp.raise_for_status()
            runs = run_resp.json().get("runs", [])
        except Exception as e:
            logger.exception(f"{cleaned_name} - Failed to fetch runs for job ID {job_id}", extra={"group_id": 0})
            continue

        if not runs:
            logger.warning(f"{cleaned_name} - No runs found for this job.", extra={"group_id": 0})
            continue

        # === Error Grouping Setup ===
        file_error_groups = {}
        file_error_counts = {}
        group_counter = 100  # Start from 100 to make log lines distinct

        for idx, run in enumerate(runs, 1):
            run_id = run["run_id"]
            state = run["state"]
            life_cycle = state.get("life_cycle_state", "N/A")
            result = state.get("result_state", "N/A")
            start_time = run.get("start_time", 0)
            readable_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time / 1000))

            logger.info(
                f"{cleaned_name} - Run {idx}: ID={run_id} | Life Cycle={life_cycle} | Result={result} | Start={readable_time}",
                extra={"group_id": 0}
            )

            try:
                task_resp = session.get(f"{host}/api/2.2/jobs/runs/get", params={"run_id": run_id})
                task_resp.raise_for_status()
                tasks = task_resp.json().get("tasks", [])
            except Exception:
                logger.exception(f"{cleaned_name} - Failed to fetch task details for run_id={run_id}", extra={"group_id": 0})
                continue

            for task in tasks:
                trun = task.get("run_id")
                key = task.get("task_key", "unknown_task")
                task_filename = key  # Remove "Task " prefix

                try:
                    output_resp = session.get(f"{host}/api/2.2/jobs/runs/get-output", params={"run_id": trun})
                    output_resp.raise_for_status()
                    data = output_resp.json()
                    error_info = data.get("error")

                    if error_info:
                        raw_error = str(error_info).replace('\n', ' ').replace('\r', '')
                        match = re.search(r'Task ([\w\-]+)', raw_error)  # Remove closing ) from regex
                        source_file = match.group(1) if match else task_filename

                        # Assign group number if not already assigned
                        if source_file not in file_error_groups:
                            file_error_groups[source_file] = group_counter
                            file_error_counts[source_file] = 0
                            group_counter += 1

                        group_id = file_error_groups[source_file]
                        file_error_counts[source_file] += 1
                        error_index = file_error_counts[source_file]

                        logger.error(
                            f"{source_file} - [{group_id} Error {error_index}] Task [{key}] run_id={trun} ERROR: {raw_error}",
                            extra={"group_id": group_id}
                        )
                    else:
                        logger.info(
                            f"{task_filename} - ✅ Task [{key}] run_id={trun} completed successfully.",
                            extra={"group_id": 0}
                        )
                except Exception:
                    logger.exception(
                        f"{task_filename} - Failed to fetch output for task [{key}] run_id={trun}",
                        extra={"group_id": 999}
                    )

    print("\n✅ All job logs saved in the 'log/' folder.")