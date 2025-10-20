# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.gold_catalog}.fin_general_ledger.dim_vcodes_hierarchy
    (
        -- Keys
        hierarchy_id STRING
            COMMENT "Hierarchy ID",
        vcode_hierarchy_node STRING
            COMMENT "The hierarchy node ID. This is the node ID at the root of the hierarchy",
        
        vcode_description STRING
            COMMENT "The description of the vcode",
        -- Payload
        hierarchy_level STRING
            COMMENT "The level in the hierarchy",
        is_leaf_node BOOLEAN
            COMMENT "Flag to indicate if this is a leaf node or not",
        hierarchy_name STRING
            COMMENT "Hierarchy Name",
        level_1_node STRING
            COMMENT "The ID of the level 1 node",
        level_1_node_text STRING
            COMMENT "The text description of the level 1 node",
        level_2_node STRING
            COMMENT "The ID of the level 2 node",
        level_2_node_text STRING
            COMMENT "The text description of the level 2 node",
        level_3_node STRING
            COMMENT "The ID of the level 3 node",
        level_3_node_text STRING
            COMMENT "The text description of the level 3 node",
        level_4_node STRING
            COMMENT "The ID of the level 4 node",
        level_4_node_text STRING
            COMMENT "The text description of the level 4 node",
        level_5_node STRING
            COMMENT "The ID of the level 5 node",
        level_5_node_text STRING
            COMMENT "The text description of the level 5 node",
        level_6_node STRING
            COMMENT "The ID of the level 6 node",
        level_6_node_text STRING
            COMMENT "The text description of the level 6 node",
        level_7_node STRING
            COMMENT "The ID of the level 7 node",
        level_7_node_text STRING
            COMMENT "The text description of the level 7 node",
        level_8_node STRING
            COMMENT "The ID of the level 8 node",
        level_8_node_text STRING
            COMMENT "The text description of the level 8 node",
        level_9_node STRING
            COMMENT "The ID of the level 9 node",
        level_9_node_text STRING
            COMMENT "The text description of the level 9 node",
        level_10_node STRING
            COMMENT "The ID of the level 10 node",
        level_10_node_text STRING
            COMMENT "The text description of the level 10 node",
        level_11_node STRING
            COMMENT "The text description of the level 11 node",
        level_11_node_text STRING
            COMMENT "The text description of the level 11 node",
        level_12_node STRING
            COMMENT "The text description of the level 12 node",
        level_12_node_text STRING
            COMMENT "The text description of the level 12 node",
        level_13_node STRING
            COMMENT "The text description of the level 13 node",
        level_13_node_text STRING
            COMMENT "The text description of the level 13 node",
        level_14_node STRING
            COMMENT "The text description of the level 14 node",
        level_14_node_text STRING
            COMMENT "The text description of the level 14 node"
        )
        CLUSTER BY 
            AUTO;
''')