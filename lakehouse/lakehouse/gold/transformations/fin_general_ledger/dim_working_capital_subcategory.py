# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

dbutils.widgets.text(
    name='metadata_filename',
    defaultValue='gold.dim.working_capital_subcategory.yaml',
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

wcsubcat_tablename = metadata.source_3partname(
    tablename='working_capital_subcategory'
)

dest_tablename = metadata.dest_3partname()

# COMMAND ----------

gold_table_query = f'''
    SELECT
        wcsubcat.category_rank,
        wcsubcat.node,
        wcsubcat.category_id,
        wcsubcat.sub_category_description,
        wcsubcat.category_description
    FROM
        {wcsubcat_tablename} AS wcsubcat
'''

# COMMAND ----------

write_result = metadata.process_dim_transformation_query(gold_table_query, False)
logger.log.info(f'Write: {dest_tablename} {write_result}')