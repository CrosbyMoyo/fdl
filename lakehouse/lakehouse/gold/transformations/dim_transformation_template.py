# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold: {Schema} Dim {Table}
# MAGIC
# MAGIC ### Description: 
# MAGIC <!--
# MAGIC For more information see: https://dev.azure.com/VivoEnergy/Azure%20SAP%20Data%20Reporting/_wiki/wikis/Azure-SAP-Data-Reporting.wiki/360/Lakehouse-Medallion-Architecture
# MAGIC -->
# MAGIC {Provide a high level description of what this transformation notebook does}
# MAGIC
# MAGIC ### Parameters: 
# MAGIC - metadata_filename: The name of the metadata YAML file for this notebook for example `silver.exchange_rates.yaml`

# COMMAND ----------

# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata

# COMMAND ----------

dbutils.widgets.text(
    name='metadata_filename',
    defaultValue='',
    label='1 - metadata_filename'
)

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name} with parameters: {dbutils.widgets.getAll()}', dbutils.widgets.getAll())
metadata_filename = dbutils.widgets.get('metadata_filename')

# COMMAND ----------

metadata = GoldMetadataYaml(
    file_path=f'./metadata/{metadata_filename}',
    slv_catalog=env_vars.silver_catalog,
    gld_catalog=env_vars.gold_catalog
)

# COMMAND ----------

#TODO: Update this with your source/s - use meaningful names for each of them!

# Read sources 
source_a = metadata.source_3partname(
    tablename='{source_a}'
)

# Read Destination
destination_tablename = metadata.dest_3partname()

# COMMAND ----------

#TODO: Add your transformation logic - use meaningful names for your temp views and CTEs!
gold_table_query = f'''
    SELECT
        a.column_a,
        a.column_a,
    FROM
        {source_a} AS a
'''

# COMMAND ----------

write_result = metadata.process_dim_transformation_query(gold_table_query)
logger.log.info(f'Write: {destination_tablename} {write_result}')