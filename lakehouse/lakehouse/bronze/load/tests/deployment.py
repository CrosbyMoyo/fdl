# Databricks notebook source
# MAGIC %md 
# MAGIC ## Deploy: Refactor Bronze Table

# COMMAND ----------

# MAGIC %run ../loaders

# COMMAND ----------

dbutils.widgets.text('env', '')
dbutils.widgets.text('source_system', '')

dbutils.widgets.text('bronze_catalog', '')
dbutils.widgets.text('bronze_schema', '')
dbutils.widgets.text('bronze_table', '')

notebook_params = dbutils.widgets.getAll()

# COMMAND ----------

# Hop 1
bronze_replica = f"{notebook_params['bronze_catalog']}.{notebook_params['bronze_schema']}.{notebook_params['bronze_table']}"

# Hop 2 
bronze_history = f"{notebook_params['bronze_catalog']}.{notebook_params['bronze_schema']}__staging.{notebook_params['bronze_table']}__history"

bronze_history_checkpoints = f"abfss://vivid@vividstorage{notebook_params['env']}.dfs.core.windows.net/metadata/offsets/{bronze_history}"

# Hop 3
bronze_table = f"{notebook_params['bronze_catalog']}.{notebook_params['bronze_schema']}.{notebook_params['bronze_table']}"

bronze_table_checkpoints = f"abfss://vivid@vividstorage{notebook_params['env']}.dfs.core.windows.net/metadata/offsets/{bronze_table}"

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

spark.sql(f"ALTER TABLE {bronze_replica} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

cdf_version = spark.sql(f"DESCRIBE HISTORY {bronze_replica}").select("version").collect()[0].version

# COMMAND ----------

#TODO: Generate & Execute DDL of History Table

# COMMAND ----------

#TODO: Generate & Execute DDL of History Table


# COMMAND ----------

# 1. Copy data from source to target 
query = f"""
    INSERT INTO {bronze_history} BY NAME
    SELECT 
        b.*,
        current_timestamp() as __etl_bronze_timestamp,
        'INSERT' as __etl_source_operation
    FROM 
        {bronze_replica} AS b
"""
print(query)

spark.sql(query).display()

# COMMAND ----------

listener = ListeningTom()
spark.streams.addListener(listener)

# COMMAND ----------

cdf_transformer = ChangeDataCaptureTransformer()

cdf_loader = StreamOrchestrator(
    source_table=bronze_replica,
    target_table=bronze_history,
    checkpoint_location=bronze_history_checkpoints,
    read_kwargs={'readChangeFeed': True, 'startingVersion': cdf_version},
    spark=spark
)

cdf_loader.add(cdf_transformer)

# COMMAND ----------

cdf_loader.run()

# COMMAND ----------

soft_delete_processor = SoftDeleteProcessor(primary_key=primary_key, source_table=bronze_history, target_table=bronze_table)

bronze_loader = StreamOrchestrator(
    source_table=bronze_history,
    target_table=bronze_table,
    checkpoint_location=bronze_table_checkpoints,
    spark=spark
)

# COMMAND ----------

bronze_loader.run(soft_delete_processor)