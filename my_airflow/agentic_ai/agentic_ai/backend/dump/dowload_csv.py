import os
from dotenv import load_dotenv
import requests
import base64

load_dotenv()  

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

def download_csv_from_dbfs(dbfs_path, local_path):
    """Download CSV from DBFS to local machine"""
    print(f"⬇️  Attempting to download: {dbfs_path}")
    
    url = f"{DATABRICKS_HOST}/api/2.0/dbfs/read"
    params = {"path": dbfs_path}
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}

    try:
        # Make the request to get the file content
        res = requests.get(url, headers=headers, params=params)

        if res.status_code == 200:
            # Decode the base64 content
            content = res.json()["data"]
            decoded = base64.b64decode(content)

            # Write the decoded content to the local file
            with open(local_path, "wb") as f:  # Open file in binary write mode
                f.write(decoded)

            print(f"✅ CSV downloaded successfully to: {local_path}")
            return True
        else:
            print(f"❌ Failed to download CSV: {res.status_code} → {res.text}")
            return False
    except Exception as e:
        print(f"⚠️ An error occurred: {str(e)}")
        return False

# Example Usage
dbfs_path = '/FileStore/corrupted_scraped_data.csv'  # Replace with your DBFS file path
local_path = 'corrupted_scraped_data.csv'  # Replace with desired local path

download_csv_from_dbfs(dbfs_path, local_path)
