
import requests
from airflow.models import Variable

import os

AIRBYTE_SERVER = "https://airbyte.inteliconvo.com"

CLIENT_ID = os.getenv("AIRBYTE_CLIENT_ID", "REPLACED_SECRET")
CLIENT_SECRET = os.getenv("AIRBYTE_CLIENT_SECRET", "REPLACED_SECRET")

# Airflow Variable key
VAR_KEY = "AIRBYTE_PAT"

def generate_airbyte_pat():
    url = f"{AIRBYTE_SERVER}/api/public/v1/applications/token"
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }

    response = requests.post(url, json=payload)

    if response.status_code != 200:
        raise Exception(f"PAT generation failed: {response.text}")

    return response.json()["access_token"]

def update_airbyte_pat_in_airflow_variable(pat):
    Variable.set(VAR_KEY, pat)

def get_airbyte_pat():
    return Variable.get(VAR_KEY)

def automate_airbyte_pat_update():
    new_pat = generate_airbyte_pat()
    update_airbyte_pat_in_airflow_variable(new_pat)
    print("PAT Updated:", new_pat)

    return new_pat


if __name__ == "__main__":
    automate_airbyte_pat_update()
