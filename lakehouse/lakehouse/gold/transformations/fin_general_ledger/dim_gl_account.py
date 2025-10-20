# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

dbutils.widgets.text(
    name='metadata_filename',
    defaultValue='gold.dim.gl_account.yaml',
    label='1 - metadata_filename'
)

# COMMAND ----------

metadata_filename = dbutils.widgets.get('metadata_filename')
logger.log.info(f'Widget: metadata_filename = "{metadata_filename}"')

# COMMAND ----------

metadata = GoldMetadataYaml(
    file_path=f'./metadata/{metadata_filename}',
    slv_catalog=env_vars.silver_catalog,
    gld_catalog=env_vars.gold_catalog
)

# COMMAND ----------

gl_account_tablename = metadata.source_3partname(
    tablename='gl_account_master',
    include_schemaversion=True
)

dest_tablename = metadata.dest_3partname(
    include_schemaversion=True
)

# COMMAND ----------

gold_table_query = f'''
    SELECT 
        glm.chart_of_accounts,
        glm.gl_account,
        glm.gl_account_description,
        glm.balance_sheet_account_flag,
        glm.group_account_number,
        glm.functional_area,
        glm.gl_account_type,
        glm.gl_account_subtype,
        glm.last_changed_timestamp
    FROM 
        {gl_account_tablename} AS glm
'''

# COMMAND ----------

write_result = metadata.process_dim_transformation_query(gold_table_query)
logger.log.info(f'Write: {dest_tablename} {write_result}')