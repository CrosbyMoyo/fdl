# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.financial_statement_item_hierarchy.yaml"
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

source_tablename = f'{env_vars.silver_catalog}.{metadata.source_2partname(include_schemaversion=True)}'

# COMMAND ----------

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", include_schemaversion=True)}'

# COMMAND ----------

spark.sql(
    f"""
        SELECT
             s.hierarchy_id
            ,s.hierarchy_name
            ,s.node_id                AS financial_statement_item_hierarchy_node
            ,s.description            AS financial_statement_item_description
            ,s.level                  AS hierarchy_level
            ,s.leaf_flag              AS is_leaf_node
            ,s.level_1_node                   
            ,s.level_1_node_text                        
            ,s.level_2_node                   
            ,s.level_2_node_text                        
            ,s.level_3_node                   
            ,s.level_3_node_text                        
            ,s.level_4_node                   
            ,s.level_4_node_text                        
            ,s.level_5_node                   
            ,s.level_5_node_text                        
            ,s.level_6_node                   
            ,s.level_6_node_text                        
            ,s.level_7_node                   
            ,s.level_7_node_text                        
            ,s.level_8_node                   
            ,s.level_8_node_text                        
            ,s.level_9_node                   
            ,s.level_9_node_text                                  
        FROM
            {source_tablename} AS s
        WHERE s.level = (
            SELECT max(t.level)
            FROM {source_tablename} AS t
            WHERE t.node_id = s.node_id
            AND t.hierarchy_id = s.hierarchy_id
        )
    """
).createOrReplaceTempView('enhanced')

# COMMAND ----------

write_result = metadata.process_transformation_table('enhanced', env_vars.silver_catalog)
logger.log.info(f'Write: {dest_tablename} {write_result}')