# Databricks notebook source
import os

# COMMAND ----------

workspace_id = dbutils.secrets.get('vivid_kv', 'powerbi-workspace-id')
dataset_id = dbutils.secrets.get('vivid_kv', 'powerbi-dataset-id')
table_name = dbutils.widgets.get('table_name')

# COMMAND ----------

import requests
from msal import ConfidentialClientApplication

# COMMAND ----------

TENANT_ID = dbutils.secrets.get('vivid_kv', 'sp-tenant-id')
CLIENT_ID = dbutils.secrets.get('vivid_kv', 'sp-client-id')
CLIENT_SECRET = dbutils.secrets.get('vivid_kv', 'sp-client-secret')
WORKSPACE_ID = workspace_id
DATASET_ID = dataset_id 
TABLE_NAME = table_name

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]
POWER_BI_API_ENDPOINT = "https://api.powerbi.com/v1.0/myorg"

app = ConfidentialClientApplication(
    CLIENT_ID,
    authority=AUTHORITY,
    client_credential=CLIENT_SECRET
)

# COMMAND ----------

token_response = app.acquire_token_for_client(scopes=SCOPE)

if "access_token" in token_response:
    access_token = token_response["access_token"]
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }

    # Refresh a specific table in the dataset
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{WORKSPACE_ID}/datasets/{DATASET_ID}/refreshes"

    body = {
        "type": "Full",
        "objects": [
            {
                "table": TABLE_NAME
            }
        ]
    }

    response = requests.post(url, headers=headers, json=body)

    if response.status_code == 202:
        print(f"Refresh started for table '{TABLE_NAME}'.")
    else:
        print(f"Failed to start refresh: {response.status_code}")
        print(response.text)

else:
    print("Failed to acquire token.")
    print(token_response.get("error_description"))
