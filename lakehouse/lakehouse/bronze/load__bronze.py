# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze: Load Bronze
# MAGIC This notebook loads data from the FiveTran extracted tables through to the change data capture (CDC) and soft delete (SD) hops of bronze. 
# MAGIC
# MAGIC The high level flow is: 
# MAGIC
# MAGIC 1. Source replica extracted via FiveTran: `{bronze_catalog}.fivetran_{src}.{bronze_table}`
# MAGIC 2. Full history of changes in CDC: `{bronze_catalog}.{src}__staging.{bronze_table}`
# MAGIC 3. Latest records with soft delete flag: `{bronze_catalog}.{src}.{bronze_table}`
# MAGIC
# MAGIC ### Parameters: 
# MAGIC - env: The environment this is executing in 
# MAGIC - bronze_catalog: The name of the bronze catalog 
# MAGIC - bronze_schema: The name of the bronze schema 
# MAGIC - bronze_table: The name of the bronze table
# MAGIC - source_system: The name of the source system in the vivid_meta tables

# COMMAND ----------

# MAGIC %run ../common/properties

# COMMAND ----------

# MAGIC %run ./load/loaders

# COMMAND ----------

# MAGIC %run ../utilities/DDL

# COMMAND ----------

dbutils.widgets.text('env', '')
dbutils.widgets.text('source_system', '')

#TODO: Drive these ones from the metadata tables
dbutils.widgets.text('bronze_catalog', '')
dbutils.widgets.text('bronze_schema', '')
dbutils.widgets.text('bronze_table', '')

notebook_params = dbutils.widgets.getAll()

# COMMAND ----------

primary_key_columns = spark.sql(f"""
    select distinct 
        source_field_name
    from
        vivid_meta.vivid_meta.vivid_field
    where
        source_table_name = '{notebook_params["bronze_table"]}'
        and source_system = '{notebook_params["source_system"]}'
        and source_field_primary_key_flag
"""
).collect()

primary_key = [key.source_field_name for key in primary_key_columns]

# COMMAND ----------

bronze_table = f"{notebook_params['bronze_catalog']}.{notebook_params['bronze_schema']}.{notebook_params['bronze_table']}"

# Hop 1 
bronze_cdc_table = f"{notebook_params['bronze_catalog']}.{notebook_params['bronze_schema']}.{notebook_params['bronze_table']}__cdc"
bronze_cdc_checkpoint = f"abfss://vivid@vividstorage{notebook_params['env']}.dfs.core.windows.net/metadata/offsets/{bronze_cdc_table}"

# Hop 2 
bronze_sd_table = f"{notebook_params['bronze_catalog']}.{notebook_params['bronze_schema']}.{notebook_params['bronze_table']}__sd"
bronze_sd_checkpoint = f"abfss://vivid@vividstorage{notebook_params['env']}.dfs.core.windows.net/metadata/offsets/{bronze_sd_table}"

logger.log.info(f"Loading from {bronze_table} with key {primary_key}")

# COMMAND ----------

listener = ListeningTom()
spark.streams.addListener(listener)

# COMMAND ----------

cdf_transformer = ChangeDataCaptureTransformer()

cdf_loader = StreamOrchestrator(
    source_table=bronze_table,
    target_table=bronze_cdc_table,
    checkpoint_location=bronze_cdc_checkpoint,
    read_kwargs={'readChangeFeed': True},
    spark=spark
)

cdf_loader.add(cdf_transformer)


# COMMAND ----------

soft_delete_processor = SoftDeleteProcessor(primary_key=primary_key, source_table=bronze_cdc_table, target_table=bronze_sd_table)

bronze_loader = StreamOrchestrator(
    source_table=bronze_cdc_table,
    target_table=bronze_sd_table,
    checkpoint_location=bronze_sd_checkpoint,
    spark=spark
)

logger.log.info(f"Starting stream from {bronze_cdc_table} to {bronze_sd_table} checkpoints stored in {bronze_cdc_checkpoint}")

# COMMAND ----------

logger.log.info(f"Starting stream from {bronze_table} to {bronze_cdc_table} checkpoints stored in {bronze_sd_checkpoint}")
cdf_loader.run()

# COMMAND ----------

logger.log.info(f"Starting stream from {bronze_cdc_table} to {bronze_sd_table} checkpoints stored in {bronze_sd_checkpoint}")
bronze_loader.run(soft_delete_processor)