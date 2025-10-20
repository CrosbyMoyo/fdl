# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

dbutils.widgets.text(
    name='metadata_filename',
    defaultValue='gold.dim.cost_center.yaml',
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

cost_center_tablename = metadata.source_3partname(
    tablename='cost_center',
    include_schemaversion=True
)

dest_tablename = metadata.dest_3partname(
    include_schemaversion=True
)

# COMMAND ----------

gold_table_query = f'''
    SELECT 
        c.controlling_area,
        c.cost_center,
        c.valid_to,
        c.budget_holder,
        c.cost_center_description,
        c.valid_from,
        c.company_code,
        c.cost_center_category,
        c.cost_center_category_description,
        c.profit_center,
        c.department
    FROM 
        {cost_center_tablename} AS c
    QUALIFY 
        ROW_NUMBER() OVER (PARTITION BY c.cost_center ORDER BY c.valid_to DESC) = 1
'''

# COMMAND ----------

write_result = metadata.process_dim_transformation_query(gold_table_query)
logger.log.info(f'Write: {dest_tablename} {write_result}')