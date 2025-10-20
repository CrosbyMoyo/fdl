# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

dbutils.widgets.text(
    name='metadata_filename',
    defaultValue='gold.dim.cost_element_hierarchy.yaml',
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

cost_element_hierarchy_tablename = metadata.source_3partname(
    tablename='cost_element_hierarchy',
    include_schemaversion=True
)

dest_tablename = metadata.dest_3partname(
    include_schemaversion=True
)


# COMMAND ----------

gold_table_query = f'''
    SELECT 
        c.hierarchy_id,
        c.cost_element_hierarchy_node,
        c.controlling_area,
        c.hierarchy_name,
        c.hierarchy_level,
        c.is_leaf_node,
        c.level_1_node,
        c.level_1_node_text,
        c.level_1_node_sort_order,
        c.level_2_node,
        c.level_2_node_text,
        c.level_2_node_sort_order,
        c.level_3_node,
        c.level_3_node_text,
        c.level_3_node_sort_order,
        c.level_4_node,
        c.level_4_node_text,
        c.level_4_node_sort_order
    FROM 
        {cost_element_hierarchy_tablename} AS c
'''

# COMMAND ----------

write_result = metadata.process_dim_transformation_query(gold_table_query)
logger.log.info(f'Write: {dest_tablename} {write_result}')