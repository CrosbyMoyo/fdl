# Databricks notebook source
# MAGIC %md 
# MAGIC ## Bronze: SAC Ingestion 
# MAGIC This Databricks notebook is responsible for loading and processing SAC (SAP Analytics Cloud) model data using the SAC API and writing the processed data to a Delta table in the Bronze layer of Unity Catalog.
# MAGIC
# MAGIC Steps performed in this notebook:
# MAGIC
# MAGIC 1. Import necessary libraries and retrieve parameters (model_id, model_name, database, date_range_start, date_range_end, and source_system) from ADF.
# MAGIC 2. Construct the full target Delta table path using the Unity Catalog bronze layer and source system.
# MAGIC 3. Establish a connection to SAC using secure credentials (client ID and secret) retrieved from a Databricks secret scope linked to Azure Key Vault.
# MAGIC 4. Loop through each month in the specified date range and apply a logical filter on the SAC model using the Date dimension and EQUAL operator.
# MAGIC 5. Retrieve fact data for each monthly period using the SAC API and accumulate non-empty results.
# MAGIC
# MAGIC For the "special_items" model, apply a predefined schema to avoid Spark inference issues due to null or inconsistent types.
# MAGIC
# MAGIC ### Parameters:
# MAGIC - model_id: The SAC model identifier to connect and extract data from.
# MAGIC - model_name: The destination table name to be created or appended in the Bronze layer.
# MAGIC - database: The logical SAC data source group (e.g., vivoenergy).
# MAGIC - date_range_start: Start year (inclusive) for monthly extraction.
# MAGIC - date_range_end: End year (exclusive) for monthly extraction.
# MAGIC - source_system: Logical Unity Catalog schema name (e.g., sap_sac).
# MAGIC - client_id and client_secret: Retrieved securely via Databricks secret scope.
# MAGIC
# MAGIC ### Note:
# MAGIC
# MAGIC Ensure the SAC API is accessible and valid credentials are configured in the Databricks secret scope.
# MAGIC
# MAGIC For the "special_items" model, an explicit schema is required to prevent type inference errors.

# COMMAND ----------

!pip install sacapi

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col
from sacapi import sacapi

# COMMAND ----------

# MAGIC %run ../common/properties

# COMMAND ----------

dbutils.widgets.text("model_id", "")      
dbutils.widgets.text("model_name", "")    
dbutils.widgets.text("source_database", "")  
dbutils.widgets.text("source_system", "")     
dbutils.widgets.text("date_range_start", "")
dbutils.widgets.text("date_range_end", "")
dbutils.widgets.text('load_type', 'full')
dbutils.widgets.text('env_name', '')

model_id = dbutils.widgets.get("model_id")
model_name = dbutils.widgets.get("model_name")
database = dbutils.widgets.get("source_database")
source_system = dbutils.widgets.get("source_system")
date_range_start = int(dbutils.widgets.get("date_range_start"))
date_range_end = int(dbutils.widgets.get("date_range_end"))
load_type = dbutils.widgets.get('load_type')
env_name = dbutils.widgets.get('env_name')

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')
logger.log.info(f'Widgets: {dbutils.widgets.getAll()}"')

# COMMAND ----------

required = {"model_id": model_id, "model_name": model_name, "database": database}
missing = [k for k, v in required.items() if not v]
if missing:
    raise ValueError(f"Missing required ADF parameter(s): {', '.join(missing)}")

# COMMAND ----------

SECRET_SCOPE = "vivid_kv"
client_id = dbutils.secrets.get(scope=SECRET_SCOPE, key="vivo-sac-sp-client-id")
client_secret = dbutils.secrets.get(scope=SECRET_SCOPE, key="vivo-sac-sp-client-secret")

# COMMAND ----------

# TODO: This should be read from metadata / db 
full_table_name = f"vivid_{env_name}_brz.{source_system}.{model_name}"

if model_name == "special_items":
    schema = StructType([
        StructField("Version", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("SpecialItems", StringType(), True),
        StructField("Amount_MTD", DoubleType(), True),
        StructField("Amount_YTD", DoubleType(), True)
    ])
else:
    schema = None

# COMMAND ----------

sac = sacapi.SACConnection(f"{database}", "eu10")
sac.connect(client_id, client_secret)

# COMMAND ----------

# TODO: Remove this when we support deltas
if load_type == 'full' and spark.catalog.tableExists(full_table_name): 
    spark.sql(f'DELETE FROM {full_table_name}')

# COMMAND ----------

for year in range(date_range_start, date_range_end):
    for month in range(1, 13):
        period = f"{year}{month:02d}"
        try:
            md = sac.getModelMetadata(model_id)
        except sacapi.RESTError as err:
            logger.log.error(f"Refreshing Token ... ")
            sac.connect(client_id, client_secret)
            md = sac.getModelMetadata(model_id)

        sac.addLogicalFilter(model_id, "Date", period, sac.filterOperators.EQUAL)
        fd = sac.getFactData(md)
        logger.log.info(f"Extracting {len(fd)} rows for model_id {model_id}, year {year}, month {month}")
        if fd:
            df = spark.createDataFrame(fd, schema) if schema else spark.createDataFrame(fd)
            df.write.format("delta").mode("append").saveAsTable(full_table_name)