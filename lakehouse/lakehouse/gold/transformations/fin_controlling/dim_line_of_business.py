# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

dbutils.widgets.text(
    name='metadata_filename',
    defaultValue='gold.dim.line_of_business.yaml',
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

line_of_business_tablename = metadata.source_3partname(
    tablename='profit_center',
    include_schemaversion=True
)

dest_tablename = metadata.dest_3partname(
    include_schemaversion=True
)

# COMMAND ----------

gold_table_query = f'''
    SELECT DISTINCT
        -- Key 
        l.line_of_business_1             AS line_of_business,
        -- Payload
        l.line_of_business_1_description AS line_of_business_description,
        l.line_of_business               AS class_of_business,
        l.line_of_business_description   AS class_of_business_description
    FROM {line_of_business_tablename} AS l
    WHERE
        l.line_of_business <> ''
        AND l.line_of_business_description <> ''
        AND l.line_of_business_1 <> ''
        AND l.line_of_business_1_description <> ''
'''

# COMMAND ----------

write_result = metadata.process_dim_transformation_query(gold_table_query)
logger.log.info(f'Write: {dest_tablename} {write_result}')