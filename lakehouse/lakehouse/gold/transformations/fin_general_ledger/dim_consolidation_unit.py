# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

dbutils.widgets.text(
    name='metadata_filename',
    defaultValue='gold.dim.consolidation_unit.yaml',
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

consolidation_unit_tablename = metadata.source_3partname(
    tablename='consolidation_unit',
    include_schemaversion=True
)

dest_tablename = metadata.dest_3partname(
    include_schemaversion=True
)

# COMMAND ----------

gold_table_query = f'''
    SELECT 
        cu.dimension,
        cu.consolidation_unit,
        cu.consolidation_unit_description,
        cu.consolidation_group,
        cu.country_region,
        cu.company
    FROM 
        {consolidation_unit_tablename} AS cu
'''

# COMMAND ----------

write_result = metadata.process_dim_transformation_query(gold_table_query)
logger.log.info(f'Write: {dest_tablename} {write_result}')