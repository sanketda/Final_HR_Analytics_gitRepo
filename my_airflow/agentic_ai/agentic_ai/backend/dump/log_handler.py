import re
import os
import pandas as pd

def log_handler_func(directory_path):
    """
    Log handler for updated log format where Taskname comes after ERROR keyword
    Example line:
    [2025-06-27 17:18:07,370] 124 - ERROR - silver_to_gold - Task [silver_to_gold] run_id=... ERROR: ...
    """
    print("dir path: "+directory_path)
    print(f"🔍 Searching for log files in: {directory_path}")
    
    # Updated regex pattern (timestamp → code → ERROR → Taskname → message)
    # Removed .py extension from Taskname pattern
    log_pattern = re.compile(
        r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\]\s*'  # timestamp
        r'(\d+)\s*-\s*'                                        # error code
        r'ERROR\s*-\s*'                                        # ERROR keyword
        r'([\w\-\.]+)\s*-\s*'                                  # Taskname (no .py extension)
        r'(.+?)(?=\n|$)',                                      # message
        re.MULTILINE
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
        return pd.DataFrame(columns=["Timestamp", "ErrorCode", "Taskname", "Message"])

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
        return pd.DataFrame(columns=["Timestamp", "ErrorCode", "Taskname", "Message"])

    matches = log_pattern.findall(log_content)
    print(f"🔍 Found {len(matches)} regex matches")
    
    if not matches:
        print("❌ No matches found with current regex pattern")
        print("📋 Showing raw log content for manual inspection:")
        print(log_content)
        return pd.DataFrame(columns=["Timestamp", "ErrorCode", "Taskname", "Message"])

    df_data = []
    for i, match in enumerate(matches):
        print(f"Match {i+1}: {match}")
        df_data.append({
            "Timestamp": match[0].strip(),
            "ErrorCode": match[1].strip(),
            "Taskname": match[2].strip(),
            "Message": match[3].strip()
        })

    df = pd.DataFrame(df_data)
    print(f"✅ Created DataFrame with {len(df)} rows")
    print("📊 DataFrame columns:", list(df.columns))
    print("📋 DataFrame preview:")
    print(df.head())
    return df

# Optional: sample test
def test_with_sample_log():
    sample_log = """
    [2025-06-27 17:18:07,370] 124 - ERROR - silver_to_gold - Task [silver_to_gold] run_id=717858328500502 ERROR: Run result unavailable: The task was skipped.
    [2025-06-27 17:20:01,120] 130 - ERROR - bronze_to_silver - Task [bronze_to_silver] run_id=123456789000001 ERROR: Path not found.
    """
    
    pattern = re.compile(
        r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\]\s*'
        r'(\d+)\s*-\s*'
        r'ERROR\s*-\s*'
        r'([\w\-\.]+)\s*-\s*'  # Removed .py extension
        r'(.+?)(?=\n|$)',
        re.MULTILINE
    )

    matches = pattern.findall(sample_log)
    for i, m in enumerate(matches, 1):
        print(f"\nMatch {i}:")
        print(f"  Timestamp: {m[0]}")
        print(f"  ErrorCode: {m[1]}")
        print(f"  Taskname:  {m[2]}")
        print(f"  Message:   {m[3]}")

if __name__ == "__main__":
    # Run this only if you want to test with actual files
    # log_handler_func("path/to/logs")  

    # For isolated testing:
    test_with_sample_log()