# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

dbutils.widgets.text(
    name='metadata_filename',
    defaultValue='gold.dim.capex_subtype.yaml',
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

cc_tablename = metadata.source_3partname(
    tablename='modelsubtype',
    include_schemaversion=True
)

dest_tablename = metadata.dest_3partname(
    include_schemaversion=True
)

# COMMAND ----------

gold_table_query = f'''
        SELECT
            cc.model_subtype_id,
            cc.subtype_name
        FROM
            {cc_tablename} AS cc
'''

# COMMAND ----------

write_result = metadata.process_dim_transformation_query(gold_table_query)
logger.log.info(f'Write: {dest_tablename} {write_result}')