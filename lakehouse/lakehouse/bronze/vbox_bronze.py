# Databricks notebook source
# MAGIC %md 
# MAGIC ## Bronze: SFTP Ingestion
# MAGIC
# MAGIC This Databricks notebook is responsible for extracting CSV files from an external SFTP server using the Paramiko library, sanitizing column names, and writing the data to a Delta table in the Bronze Layer of Unity Catalog.
# MAGIC
# MAGIC Steps performed in this notebook:
# MAGIC
# MAGIC 1. **Retrieve parameters** from Azure Data Factory via widgets (e.g., `directory`, `filename`, `host_name`, `source_database`, `source_system`, `source_table`).
# MAGIC 2. **Connect to the SFTP server** using Paramiko with configured credentials and securely download the specified file as a byte stream.
# MAGIC 3. **Decode the file** content while handling encoding issues (e.g., `utf-8`, fallback to `cp1252`) and write the raw content to Azure Data Lake Storage (ADLS) in a staging path.
# MAGIC 4. **Read the CSV** file into a Spark DataFrame, applying `.option("header", True)` and sanitizing column names by:
# MAGIC    - Stripping leading/trailing whitespace
# MAGIC    - Replacing spaces and special characters with underscores
# MAGIC 5. **Write the sanitized data** as a Delta table to the appropriate schema and table in Unity Catalog's Bronze layer.
# MAGIC
# MAGIC ### Parameters:
# MAGIC - `source_system`: Used as the Unity Catalog schema name and part of the ADLS folder path.
# MAGIC - `source_database`: Logical source grouping for the data.
# MAGIC - `source_table`: Final Delta table name in Unity Catalog.
# MAGIC - `directory`: SFTP subdirectory containing the target file.
# MAGIC - `filename`: Exact name of the file to ingest (e.g., `GL Account - Excluded.csv`).
# MAGIC
# MAGIC ### Output:
# MAGIC - Delta table: `<BRZ_CATALOG>.<source_system>.<source_table>`
# MAGIC - Raw file: Written to staging path `abfss://.../staging/<source_system>_<source_database>_<source_table>/<timestamp>/<source_table>.parquet`
# MAGIC
# MAGIC ### Note:
# MAGIC This notebook enforces **column name sanitization** and assumes all columns will be cleaned before writing to Delta. No schema inference overrides are applied — ensure data quality at source or handle with schema enforcement downstream.
# MAGIC

# COMMAND ----------

!pip install paramiko 

# COMMAND ----------

from datetime import datetime
import paramiko
import os
import re

# COMMAND ----------

# MAGIC %run ../common/properties

# COMMAND ----------

dbutils.widgets.text("source_system", "")
dbutils.widgets.text("source_database", "")
dbutils.widgets.text("source_table", "")
dbutils.widgets.text("directory", "")
dbutils.widgets.text("filename", "")
dbutils.widgets.text("hostname", "")
dbutils.widgets.text("staging_path", "")
dbutils.widgets.text('load_type', 'full')
dbutils.widgets.text('table_alias', "")
dbutils.widgets.text('csv_delimiter', ',')

source_system = dbutils.widgets.get("source_system")
source_database = dbutils.widgets.get("source_database")
source_table = dbutils.widgets.get("source_table")
directory = dbutils.widgets.get("directory")
filename = dbutils.widgets.get("filename")
sftp_host = dbutils.widgets.get("hostname")
staging_path = dbutils.widgets.get("staging_path")
load_type = dbutils.widgets.get('load_type')
table_alias = dbutils.widgets.get('table_alias')
csv_delimiter = dbutils.widgets.get('csv_delimiter')

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')
logger.log.info(f'Widgets: {dbutils.widgets.getAll()}"')

# COMMAND ----------

BRZ_CATALOG = os.getenv("BRZ_CATALOG")
env = os.getenv("ENV")

SECRET_SCOPE = "vivid_kv"
sftp_user = dbutils.secrets.get(scope=SECRET_SCOPE, key="vivo-vbox-basic-username")
sftp_pass = dbutils.secrets.get(scope=SECRET_SCOPE, key="vivo-vbox-basic-password")
execution_ts = datetime.now().strftime("%Y%m%d%H%M%S")
sftp_port = 22

# COMMAND ----------

remote_file_path = f"/{directory}/{filename}.csv"

transport = paramiko.Transport((sftp_host, sftp_port))
transport.connect(username=sftp_user, password=sftp_pass)
sftp = paramiko.SFTPClient.from_transport(transport)

# TODO: Set this to append when we support deltas 
write_mode = 'overwrite' if load_type == 'full' else 'append'

# COMMAND ----------

remote_file = sftp.file(remote_file_path, mode='r')
content_bytes = remote_file.read()
try:
    content_string = content_bytes.decode("utf-8")
except:
    content_string = content_bytes.decode("cp1252")

# COMMAND ----------

filename = f"{source_table}.parquet"

# COMMAND ----------

dbutils.fs.put(f"abfss://vivid@vividstorage{env}.dfs.core.windows.net/staging/{staging_path}/{filename}", content_string, True)

# COMMAND ----------

def sanitize_column(col):
    col = col.strip()   
    col = re.sub(r"[^\w]", "_", col) 
    col = re.sub(r"_+", "_", col) 
    col = col.strip("_")
    return col

# COMMAND ----------

df = spark.read.option("header", True).option("delimiter", csv_delimiter).csv(f"abfss://vivid@vividstorage{env}.dfs.core.windows.net/staging/{staging_path}/{filename}")
df_clean = df.toDF(*[sanitize_column(c) for c in df.columns])

# COMMAND ----------

final_table_name = table_alias if source_system.lower() == "ftp_vbox" and table_alias else source_table.lower()

df_clean.write.option("mergeSchema", "true").mode(write_mode).saveAsTable(f"{BRZ_CATALOG}.{source_system.lower()}.{final_table_name.lower()}")