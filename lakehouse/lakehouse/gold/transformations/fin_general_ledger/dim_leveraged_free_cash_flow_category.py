# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

dbutils.widgets.text(
    name='metadata_filename',
    defaultValue='gold.dim.leveraged_free_cash_flow_category.yaml',
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

lfcfcat_tablename = metadata.source_3partname(
    tablename='leveraged_free_cash_flow_category'
)

dest_tablename = metadata.dest_3partname()

# COMMAND ----------

gold_table_query = f'''
    SELECT
        lfcfcat.rank,
        lfcfcat.parent_id,
        lfcfcat.lfcf_category_node,
        lfcfcat.lfcf_category
    FROM
        {lfcfcat_tablename} AS lfcfcat
'''

# COMMAND ----------

write_result = metadata.process_dim_transformation_query(gold_table_query, False)
logger.log.info(f'Write: {dest_tablename} {write_result}')