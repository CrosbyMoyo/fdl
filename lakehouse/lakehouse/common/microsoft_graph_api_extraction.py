# Databricks notebook source
import requests

# COMMAND ----------

# MAGIC %run ./properties

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

def fetch_all_pages(url, headers):
    all_results = []
    while url:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Ensure HTTP request was successful
        data = response.json()
        
        # Append current page results
        if "value" in data:
            all_results.extend(data["value"])
        
        # Get next page URL if exists, otherwise stop
        url = data.get("@odata.nextLink")
    
    return all_results

# COMMAND ----------

SECRET_SCOPE = "vivid_kv"

CLIENT_ID = dbutils.secrets.get(scope=SECRET_SCOPE, key="sp-client-id")
TENANT_ID = dbutils.secrets.get(scope=SECRET_SCOPE, key="sp-tenant-id")
CLIENT_SECRET = dbutils.secrets.get(scope=SECRET_SCOPE, key="sp-client-secret")

AUTH_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
GRAPH_API_URL = "https://graph.microsoft.com/v1.0"

logger.log.info("Started process.")

company_code_list = [
    "AE01", "AEL1", "BF01", "BF02", "BW01", "BW02", "BWC1", "BWH1", "CIL2", "CD01", "CI01", "CIL1", "CONS", "CV01", "CV02", "DCON", "GA01", "GH01", "GHH1", "GHL1",
    "GN01", "GN02", "GNL1", "GNL2", "KE01", "KE04", "KE05", "KEH1", "KEL1", "LS01", "LS02", "MA01", "MA02", "MA03", "MA04", "MAL1",
    "MAL2", "MG01", "ML01", "ML02", "ML03", "MU01", "MU02", "MU03", "MU04", "MU05", "MUH1", "MUH2", "MUH3", "MUH4", "MUH5", "MUH6", "MUH7", "MUH8", "MUH9", 
    "MUHC", "MUHT", "MW01", "MZ01", "MZ02", "NA01", "NA02", "NG01", "NG02", "NL02", "NLH1", "NLH2", "NLH3", "NLH4",
    "NLH5", "NLH6", "NLH7", "NLH8", "NLH9", "NLHA", "NLHB", "NLHC", "NLHD", "NLHE", "NLHF", "NLHG", "NLL1", "RE01",
    "RE02", "RW01", "RW02", "SCON", "SN01", "SN02", "SN04", "SZ01", "TN01", "TN02", "TN03", "TN04", "TNCO", "TNL1",
    "TNL2", "TOPC", "TZ01", "TZ02", "TZ03", "UG01", "UK01", "UK02", "UKH1", "YT01", "YT02", "YTH1", "ZA02", "ZA03",
    "ZA04", "ZA05", "ZA06", "ZA07", "ZA08", "ZA09", "ZA10", "ZA11", "ZA12", "ZA13", "ZA14", "ZA15", "ZAC1", "ZAC2", "ZAH1", "ZAH2",
    "ZAH3", "ZM01", "ZM02", "ZW01", "ZW02", "ZWH1", "ZWH2", "ZZ01"
]

# Get Access Token
auth_payload = {
    "grant_type": "client_credentials",
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "scope": "https://graph.microsoft.com/.default"
}
token_response = requests.post(AUTH_URL, data=auth_payload).json()
ACCESS_TOKEN = token_response.get("access_token")
HEADERS = {"Authorization": f"Bearer {ACCESS_TOKEN}", "ConsistencyLevel": "eventual"}

# Fetch groups with pagination
groups_data = []
next_link = "https://graph.microsoft.com/v1.0/groups"
while next_link:
    groups_response = requests.get(next_link, headers=HEADERS)
    response_json = groups_response.json()
    groups_data.extend(response_json["value"])
    next_link = response_json.get("@odata.nextLink")

logger.log.info(f"Fetched {len(groups_data)} groups.")

# Filter groups based on company_code_list
filtered_groups = [group for group in groups_data if group["displayName"] in company_code_list]

# Fetch users for each group with pagination
group_user_data = []
for group in filtered_groups:
    prefix = group["displayName"] if group["displayName"] in company_code_list else None
    if prefix:
        next_link = f"https://graph.microsoft.com/v1.0/groups/{group['id']}/members"
        while next_link:
            members_response = requests.get(next_link, headers=HEADERS)
            response_json = members_response.json()
            members = response_json["value"]
            print(f"Group '{group['displayName']}' has {len(members)} members.")
            for user in members:
                if user["@odata.type"] == "#microsoft.graph.user":
                    group_user_data.append({
                        "prefix": ''.join([i for i in prefix if not i.isdigit()]),
                        "userId": user["id"],
                        "userPrincipalName": user["userPrincipalName"]
                    })
            next_link = response_json.get("@odata.nextLink")

group_user_df = spark.createDataFrame(group_user_data)
distinct_df = group_user_df.select("prefix", "userPrincipalName").distinct()

# COMMAND ----------

(
    distinct_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{env_vars.meta_catalog}.vivid_meta.rls_policy")
)