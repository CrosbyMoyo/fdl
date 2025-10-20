# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold: {Schema} Fact {Table}
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

# Read source/s 
silver_table = metadata.source_3partname(
    tablename='{silver_table}'
)

# Read Destination
gold_tablename = metadata.dest_3partname()

# COMMAND ----------

#TODO: Add your transformation logic - use meaningful names for your temp views and CTEs!
gold_fact = spark.sql(f'''
    SELECT 
        s.*
    FROM 
        silver_table AS s 
''')

gold_table.createOrReplaceTempView('gold_fact')

# COMMAND ----------

# TODO: Pull out your business keys and measures
spark.sql(f'''
    SELECT
        -- Foreign Keys
        ,{metadata.get_fkey_ddl(["f.business_key_a"])} AS dimension_a_skey
        ,{metadata.get_fkey_ddl(["f.business_key_b"])} AS dimension_b_skey

        -- Payload
        ,f.measure_a
        ,f.measure_b
    FROM 
        gold_fact AS f
''').createOrReplaceTempView('gold_table')

# COMMAND ----------

etl_fields = spark.sql(f'''
    {metadata.get_etl_fields_ddl('gold_table')}
''')
etl_fields.createOrReplaceTempView('etl_fields')

# COMMAND ----------

metadata.insert_overwrite('etl_fields', destination)