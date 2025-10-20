# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

dbutils.widgets.text(
    name='metadata_filename',
    defaultValue='gold.dim.material.yaml',
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

m_tablename = metadata.source_3partname(
    tablename='material_master',
    include_schemaversion=True
)

dest_tablename = metadata.dest_3partname(
    include_schemaversion=True
)

# COMMAND ----------

gold_table_query = f'''
    SELECT
        m.material_number
        ,m.material_type
        ,m.industry_sector 
        ,m.material_group
        ,m.base_unit_of_measure
        ,m.labor_office
        ,m.volume
        ,m.division
        ,m.length
        ,m.product_hierarchy
        ,m.external_material_group
        ,m.transportation_group
        ,m.manufacturer_book_part_number
    FROM
        {m_tablename} AS m
'''

# COMMAND ----------

write_result = metadata.process_dim_transformation_query(gold_table_query)
logger.log.info(f'Write: {dest_tablename} {write_result}')