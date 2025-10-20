# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.posting_level_hierarchy.yaml"
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

source_tablename = f'{env_vars.silver_catalog}.{metadata.source_2partname(include_schemaversion=True)}'

# COMMAND ----------

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", include_schemaversion=True)}'

# COMMAND ----------

posting_level = spark.sql(
    f"""
        SELECT
             s.hierarchy_id
            ,s.hierarchy_name
            ,CASE
                WHEN s.node_id IS NOT NULL 
                AND s.leaf_flag = TRUE 
                    THEN right(s.node_id, 2)
                ELSE s.node_id
            END AS posting_level_hierarchy_node
            ,s.description            AS posting_level_description      
            ,s.level                  AS hierarchy_level
            ,s.leaf_flag              AS is_leaf_node
            ,s.level_1_node                   
            ,s.level_1_node_text                        
            ,s.level_2_node                   
            ,s.level_2_node_text                        
            ,s.level_3_node
            ,s.level_3_node_text                
                                                   
        FROM
            {source_tablename} AS s
        WHERE s.level = (
            SELECT max(t.level)
            FROM {source_tablename} AS t
            WHERE t.node_id = s.node_id
            AND t.hierarchy_id = s.hierarchy_id
        )
    """
)
posting_level.createOrReplaceTempView('posting_level')

# COMMAND ----------

enhanced = spark.sql(
    f"""
        SELECT
             s.hierarchy_id
            ,s.hierarchy_name
            ,s.posting_level_hierarchy_node
            ,CASE
                WHEN s.posting_level_description IS NULL
                    THEN dd.short_description
                ELSE s.level_3_node_text
            END AS posting_level_description      
            ,s.hierarchy_level               
            ,s.is_leaf_node             
            ,s.level_1_node                   
            ,s.level_1_node_text                        
            ,s.level_2_node                   
            ,s.level_2_node_text                        
            ,s.posting_level_hierarchy_node AS level_3_node
            ,CASE
                WHEN s.level_3_node_text IS NULL
                    THEN dd.short_description
                ELSE s.level_3_node_text
            END AS level_3_node_text                   
                                                   
        FROM
            posting_level AS s
        LEFT JOIN
            {env_vars.silver_catalog}.ca_cross_application_components.domain_descriptions AS dd
        ON
            posting_level_hierarchy_node = dd.lower_value
            AND dd.domain_name = 'FC_PLEVL'
    """
)
enhanced.createOrReplaceTempView('enhanced')

# COMMAND ----------

write_result = metadata.process_transformation_table('enhanced', env_vars.silver_catalog)
logger.log.info(f'Write: {dest_tablename} {write_result}')