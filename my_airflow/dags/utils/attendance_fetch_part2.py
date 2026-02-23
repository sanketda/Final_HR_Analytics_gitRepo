
# #---------------------------------------------------> with 100th index
import os
import re
import copy
import requests
from datetime import datetime, timedelta
from utils.zoho_config import (
    AIRBYTE_SERVER, #AIRBYTE_PAT,
    CLIENT_ID_ZOHO, CLIENT_SECRET_ZOHO, REFRESH_TOKEN_ZOHO, SOURCE_ID
)


# Step 1: Use PAT directly
# def get_access_token():
#     return AIRBYTE_PAT

from utils.airbyte_pat_utils import get_airbyte_pat
AIRBYTE_PAT = get_airbyte_pat()

# from utils.airbyte_pat_utils import get_airbyte_token 


# Step 2: Retrieve Source Details
def get_source_details(access_token):
    url = f"{AIRBYTE_SERVER}/api/v1/sources/get"
    payload = {"sourceId": SOURCE_ID, "includeConfiguration": True}
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    try:
        resp = requests.post(url, json=payload, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        if "connectionConfiguration" in data:
            return data
        elif "source" in data and data["source"].get("connectionConfiguration"):
            return data["source"]
        elif "data" in data and data["data"].get("source", {}).get("connectionConfiguration"):
            return data["data"]["source"]
        print("⚠️ No connectionConfiguration found in response:", data)
        return None
    except requests.exceptions.RequestException as e:
        print("❌ Failed to retrieve source details:", e)
        return None


# Step 3: Clean and Update Source Config
def update_source_configuration(access_token, source_details, new_config):
    url = f"{AIRBYTE_SERVER}/api/v1/sources/update"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    clean_config = copy.deepcopy(new_config)

    # 🧹 Clean invalid secret references
    def clean_dict(d):
        for k, v in list(d.items()):
            if isinstance(v, str) and re.match(r"^airbyte_workspace_.*_secret_.*_v\d+$", v):
                d[k] = None
            elif isinstance(v, dict):
                if "_secret" in v and str(v["_secret"]).startswith("airbyte_workspace_"):
                    d[k] = None
                else:
                    clean_dict(v)
            elif isinstance(v, list):
                for item in v:
                    if isinstance(item, dict):
                        clean_dict(item)
        return d

    clean_config = clean_dict(clean_config)

    # Inject Zoho credentials
    clean_config.update({
        "client_id_2": CLIENT_ID_ZOHO,
        "client_secret_2": CLIENT_SECRET_ZOHO,
        "client_refresh_token": REFRESH_TOKEN_ZOHO,
        "data_center": "in",
        "__injected_declarative_manifest": {},
    })

    workspace_id = source_details.get("workspaceId")
    source_definition_id = source_details.get("sourceDefinitionId")

    payload = {
        "sourceId": SOURCE_ID,
        "workspaceId": workspace_id,
        "sourceDefinitionId": source_definition_id,
        "connectionConfiguration": clean_config,
        "name": source_details.get("name", "Updated Zoho Source V1 SSL"),
    }

    print(f"🧾 Updating Zoho Source {SOURCE_ID} (workspace {workspace_id})...")
    resp = requests.post(url, json=payload, headers=headers)
    if resp.status_code >= 400:
        print("❌ Failed to update source configuration:", resp.status_code, resp.text)
        return None
    print("✅ Cleaned config sent successfully!")
    return resp.json()


# Step 4: Generate date range (yesterday)
def get_yesterday_dates():
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d"), yesterday.strftime("%Y-%m-%d")
    

# Step 5: Main function
def run_sourcedynamic_2nd_itr():
    try:
        print("🔹 Fetching access token...")
        access_token = AIRBYTE_PAT #get_airbyte_token() #AIRBYTE_PAT #get_access_token()

        print("🔹 Retrieving source details...")
        source_details = get_source_details(access_token)
        if not source_details:
            print("❌ Unable to retrieve source details.")
            return False

        print("🔹 Updating source configuration dynamically...")
        current_config = (
            source_details.get("connectionConfiguration")
            or source_details.get("source", {}).get("connectionConfiguration")
            or source_details.get("data", {}).get("source", {}).get("connectionConfiguration")
        )

        if not current_config:
            print("❌ connectionConfiguration not found in source_details response")
            return False

        sdate, edate = get_yesterday_dates()
        current_config.update({
            "s_i": 100,
            "sdate": sdate,
            "edate": edate,
        })

        update_response = update_source_configuration(access_token, source_details, current_config)
        if update_response:
            print("✅ Source configuration updated successfully!")
            return True
        else:
            print("❌ Failed to update source configuration.")
            return False

    except Exception as e:
        print("⚠️ Error in run_sourcedynamic:", e)
        return False


if __name__ == "__main__":
    run_sourcedynamic_2nd_itr()

