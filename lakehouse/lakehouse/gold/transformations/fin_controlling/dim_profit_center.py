# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata
# MAGIC

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

dbutils.widgets.text(
    name='metadata_filename',
    defaultValue='gold.dim.profit_center.yaml',
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

profit_center_tablename = metadata.source_3partname(
    tablename='profit_center',
    include_schemaversion=True
)

dest_tablename = metadata.dest_3partname(
    include_schemaversion=True
)

# COMMAND ----------

gold_table_query = f'''
    SELECT 
        p.profit_center,
        p.profit_center_description,
        p.controlling_area,
        p.segment,
        p.segment_description,
        p.line_of_business,
        p.line_of_business_description,
        p.line_of_business_1,
        p.line_of_business_1_description,
        p.volume_flag_ind,
        p.sales_organization,
        p.sales_organization_description,
        p.distribution_channel,
        p.distribution_channel_description,
        p.division,
        p.division_description
    FROM {profit_center_tablename} AS p
'''

# COMMAND ----------

write_result = metadata.process_dim_transformation_query(gold_table_query)
logger.log.info(f'Write: {dest_tablename} {write_result}')