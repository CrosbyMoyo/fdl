# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver: {Schema} {Table} {Hop}
# MAGIC
# MAGIC ### Description: 
# MAGIC <!--
# MAGIC For more information see: https://dev.azure.com/VivoEnergy/Azure%20SAP%20Data%20Reporting/_wiki/wikis/Azure-SAP-Data-Reporting.wiki/360/Lakehouse-Medallion-Architecture
# MAGIC -->
# MAGIC {#TODO: Provide a high level description of what this transformation notebook does}
# MAGIC
# MAGIC ### Parameters: 
# MAGIC - metadata_filename: The name of the metadata YAML file for this notebook for example `silver.exchange_rates.yaml`

# COMMAND ----------

# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')
dbutils.widgets.text('metadata_filename', '')

# COMMAND ----------

metadata_filename = dbutils.widgets.get('metadata_filename')
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')
metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

# Read sources 
#TODO: Update sources as per metadata
source_a = f'{env_vars.silver_catalog}.{metadata.sources_2partname("source_a", True)}'
source_b = f'{env_vars.silver_catalog}.{metadata.sources_2partname("source_b", True)}'

# Destination 
dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", True)}'

# COMMAND ----------

#TODO: Add your transformation logic - use meaningful names for your temp views and CTEs!
source_a_filtered = spark.sql(f'''
    SELECT 
        a.* 
    FROM 
        {source_a} AS a 
    WHERE 
        ... 
''')

source_a_filtered.createOrReplaceTempView('source_a_filtered')

# COMMAND ----------

source_b_filtered = spark.sql(f'''
    SELECT 
        b.* 
    FROM 
        {source_b} AS b
    WHERE 
        ... 
''')

source_a_filtered.createOrReplaceTempView('source_a_filtered')

# COMMAND ----------

enhanced = spark.sql(f'''
        SELECT
            a.*
        FROM 
            source_a_filtered AS a
    UNION ALL 
        SELECT
            b.*
        FROM 
            source_b_filtered AS b
''')

enhanced.createOrReplaceTempView('enhanced')

# COMMAND ----------

write_result = metadata.process_transformation_table('enhanced', env_vars.silver_catalog)
logger.log.info(f'Write: {dest_tablename} {write_result}')