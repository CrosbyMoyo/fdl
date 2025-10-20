# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.gold_catalog}.fin_controlling.dim_profit_center_hierarchy
    (
        -- Keys
        hierarchy_id STRING
            COMMENT "Hierarchy ID",
        profit_center_hierarchy_node STRING
            COMMENT "The profit center hierarchy node ID. This is the node ID at the root of the hierarchy",
        -- Payload
        hierarchy_level STRING
            COMMENT "The level in the hierarchy",
        is_leaf_node BOOLEAN
            COMMENT "Flag to indicate if this is a leaf node or not",
        controlling_area STRING
            COMMENT "Controlling area",
        hierarchy_name STRING
            COMMENT "Hierarchy Name",
        level_1_node STRING
            COMMENT "The ID of the level 2 node",
        level_1_node_text STRING
            COMMENT "The text description of the level 2 node",
        level_1_node_sort_order STRING
            COMMENT "The sort order for the level 2 node",
        level_2_node STRING
            COMMENT "The ID of the level 2 node",
        level_2_node_text STRING
            COMMENT "The text description of the level 2 node",
        level_2_node_sort_order STRING
            COMMENT "The sort order for the level 2 node",
        level_3_node STRING
            COMMENT "The ID of the level 3 node",
        level_3_node_text STRING
            COMMENT "The text description of the level 3 node",
        level_3_node_sort_order STRING
            COMMENT "The sort order for the level 3 node",
        level_4_node STRING
            COMMENT "The ID of the level 4 node",
        level_4_node_text STRING
            COMMENT "The text description of the level 4 node",
        level_4_node_sort_order STRING
            COMMENT "The sort order for the level 4 node",
        level_5_node STRING
            COMMENT "The ID of the level 5 node",
        level_5_node_text STRING
            COMMENT "The text description of the level 5 node",
        level_5_node_sort_order STRING
            COMMENT "The sort order for the level 5 node",
        level_6_node STRING
            COMMENT "The ID of the level 6 node",
        level_6_node_text STRING
            COMMENT "The text description of the level 6 node",
        level_6_node_sort_order STRING
            COMMENT "The sort order for the level 6 node"
    )
    CLUSTER BY 
        AUTO;
''')