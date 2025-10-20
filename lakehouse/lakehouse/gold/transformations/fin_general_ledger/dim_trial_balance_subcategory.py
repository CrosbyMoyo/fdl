# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

dbutils.widgets.text(
    name='metadata_filename',
    defaultValue='gold.dim.trial_balance_subcategory.yaml',
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

tbs_tablename = metadata.source_3partname(
    tablename='trial_balance_subcategory'
)

dest_tablename = metadata.dest_3partname()

# COMMAND ----------

gold_table_query = f'''
    SELECT
        tbs.category_rank,
        tbs.node,
        tbs.category_id,
        tbs.sub_category_description,
        tbs.category_description
    FROM
        {tbs_tablename} AS tbs
'''

# COMMAND ----------

write_result = metadata.process_dim_transformation_query(gold_table_query, False)
logger.log.info(f'Write: {dest_tablename} {write_result}')