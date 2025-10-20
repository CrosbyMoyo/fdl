# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')


# COMMAND ----------

dbutils.widgets.text(
    name='metadata_filename',
    defaultValue='gold.dim.posting_level_hierarchy.yaml',
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

posting_level_hierarchy_tablename = metadata.source_3partname(
    tablename='posting_level_hierarchy',
    include_schemaversion=True
)

dest_tablename = metadata.dest_3partname(
    include_schemaversion=True
)

# COMMAND ----------

gold_table_query = (
    f'''
        SELECT 
            fsi.hierarchy_id,
            fsi.hierarchy_name,
            fsi.posting_level_hierarchy_node,
            fsi.posting_level_description,
            fsi.hierarchy_level,
            fsi.is_leaf_node,
            fsi.level_1_node,
            fsi.level_1_node_text,
            fsi.level_2_node,
            fsi.level_2_node_text,
            fsi.level_3_node,
            fsi.level_3_node_text
        FROM 
            {posting_level_hierarchy_tablename} AS fsi
    '''
)

# COMMAND ----------

write_result = metadata.process_dim_transformation_query(gold_table_query)
logger.log.info(f'Write: {dest_tablename} {write_result}')