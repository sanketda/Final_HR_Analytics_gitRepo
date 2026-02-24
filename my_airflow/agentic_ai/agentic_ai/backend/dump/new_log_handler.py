import re
import os
import pandas as pd

def log_handler_func(directory_path):
    """
    Log handler that processes logs, extracts structured error details,
    merges run id and timestamp, and writes a tab-separated .txt file.
    """
    print("dir path:", directory_path)
    print(f"🔍 Searching for log files in: {directory_path}")

    job_id_pattern = re.compile(
        r'ID:\s*(\d+)'
    )

    run_info_pattern = re.compile(
        r'Run\s*\d+:\s*ID=(\d+).*?Start=([0-9\-: ]+)'
    )

    error_line_pattern = re.compile(
        r'\[(.*?)\]\s*'
        r'(\d+)\s*-\s*'
        r'ERROR\s*-\s*'
        r'([\w\-\.]+)\s*-\s*'
        r'(.+?)(?=\n|$)',
        re.MULTILINE
    )

    message_pattern = re.compile(
        r'Task\s*\[([^\]]+)\]\s*run_id=(\d+)\s*ERROR:\s*(.+)'
    )

    def get_latest_file(directory):
        try:
            if not os.path.exists(directory):
                print(f"❌ Directory does not exist: {directory}")
                return None

            files = [os.path.join(directory, f) for f in os.listdir(directory)]
            files = [f for f in files if os.path.isfile(f)]

            print(f"📂 Found {len(files)} files in directory")
            for f in files:
                print(f"   - {os.path.basename(f)} (modified: {os.path.getmtime(f)})")

            if not files:
                print("❌ No files found in directory")
                return None

            latest_file = max(files, key=os.path.getmtime)
            print(f"📄 Latest file selected: {os.path.basename(latest_file)}")
            return latest_file

        except Exception as e:
            print(f"❌ Error accessing directory: {e}")
            return None

    recent_log_file = get_latest_file(directory_path)
    if not recent_log_file:
        print("❌ No log file found, returning empty DataFrame")
        return pd.DataFrame(columns=["run", "job_id", "task_name", "message"])

    try:
        print(f"📖 Reading log file: {recent_log_file}")
        with open(recent_log_file, "r", encoding="utf-8") as f:
            log_content = f.read()

        print(f"📝 Log content length: {len(log_content)} characters")
        print("📋 First 500 characters of log content:")
        print("-" * 50)
        print(log_content[:500])
        print("-" * 50)

    except Exception as e:
        print(f"❌ Error reading log file: {e}")
        return pd.DataFrame(columns=["runs", "job_id", "task_name", "message"])

    job_id_match = job_id_pattern.search(log_content)
    job_id = job_id_match.group(1) if job_id_match else "UNKNOWN"
    print(f"✅ Detected Job ID: {job_id}")

    df_data = []
    current_run_id = None
    current_start_timestamp = None

    for line in log_content.splitlines():
        line = line.strip()
        if not line:
            continue

        run_info_match = run_info_pattern.search(line)
        if run_info_match:
            current_run_id = run_info_match.group(1).strip()
            current_start_timestamp = run_info_match.group(2).strip()
            print(f"ℹ️  Set current run: {current_run_id} | Start: {current_start_timestamp}")
            continue

        error_match = error_line_pattern.match(line)
        if error_match:
            log_timestamp = error_match.group(1).strip()
            error_code = error_match.group(2).strip()
            taskname = error_match.group(3).strip()
            message_text = error_match.group(4).strip()

            m = message_pattern.search(message_text)
            if m:
                taskname = m.group(1).strip()
                run_id_in_message = m.group(2).strip()
                error_message = m.group(3).strip()
            else:
                run_id_in_message = None
                error_message = message_text

            df_data.append({
                "Run ID": run_id_in_message or current_run_id,
                "Start Timestamp": current_start_timestamp,
                "Job ID": job_id,
                "Task Name": taskname,
                "Message": error_message
            })

    if not df_data:
        print("❌ No ERROR entries found.")
        return pd.DataFrame(columns=["runs", "job_id", "task_name", "message"])

    df = pd.DataFrame(df_data)

    # Merge Run ID and Start Timestamp
    df["runs"] = df.apply(
        # lambda row: f"{row['Run ID']}_{row['Start Timestamp']}"
        lambda row: f"{row['Run ID']}_{row['Start Timestamp']}"
        if pd.notnull(row['Run ID']) and pd.notnull(row['Start Timestamp'])
        else "",
        axis=1
    )

    # Keep and rename columns
    df = df[["runs", "Job ID", "Task Name", "Message"]]
    df = df.rename(columns={
        "Job ID": "job_id",
        "Task Name": "task_name",
        "Message": "message"
    })



    # output_file = os.path.join("E:/Academics/TY/Shyena Tech Yarns/backend", "parsed_log_output.txt")
    # df.to_csv(output_file, sep="\t", index=False, encoding="utf-8")
    # print(f"✅ DataFrame with {len(df)} rows saved to {output_file}")

    return df





if __name__ == "__main__":
    # Example usage:
    log_handler_func("log")
