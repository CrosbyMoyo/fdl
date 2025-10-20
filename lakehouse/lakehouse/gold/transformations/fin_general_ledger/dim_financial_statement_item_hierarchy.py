# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')


# COMMAND ----------

dbutils.widgets.text(
    name='metadata_filename',
    defaultValue='gold.dim.financial_statement_item_hierarchy.yaml',
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

financial_statement_item_hierarchy_tablename = metadata.source_3partname(
    tablename='financial_statement_item_hierarchy',
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
            fsi.financial_statement_item_hierarchy_node,
            fsi.financial_statement_item_description,
            fsi.hierarchy_level,
            fsi.is_leaf_node,
            fsi.level_1_node,
            fsi.level_1_node_text,
            fsi.level_2_node,
            fsi.level_2_node_text,
            fsi.level_3_node,
            fsi.level_3_node_text,
            fsi.level_4_node,
            fsi.level_4_node_text,
            fsi.level_5_node,
            fsi.level_5_node_text,
            fsi.level_6_node,
            fsi.level_6_node_text,
            fsi.level_7_node,
            fsi.level_7_node_text,
            fsi.level_8_node,
            fsi.level_8_node_text,
            fsi.level_9_node,
            fsi.level_9_node_text
        FROM 
            {financial_statement_item_hierarchy_tablename} AS fsi
    '''
)

# COMMAND ----------

write_result = metadata.process_dim_transformation_query(gold_table_query)
logger.log.info(f'Write: {dest_tablename} {write_result}')