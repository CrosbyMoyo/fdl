# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.silver_catalog}.fin_controlling.cost_center_hierarchy (
            -- key
            hierarchy_id STRING
                COMMENT 'Hierarchy ID',
            cost_center_hierarchy_node STRING 
                COMMENT 'Node',

            -- payload
            controlling_area STRING
                COMMENT 'Controlling Area',
            hierarchy_name STRING
                COMMENT 'Hierarchy Name',
            hierarchy_level INT
                COMMENT 'Hierarchy Level',
            is_leaf_node BOOLEAN
                COMMENT 'Is a leaf node?',
            level_1_node STRING
                COMMENT 'The level 1 node',
            level_1_node_text STRING
                COMMENT 'The text for the level 1 node',
            level_1_node_sort_order STRING
                COMMENT 'The sort order for the level 1 node',
            level_2_node STRING
                COMMENT 'The level 2 node',
            level_2_node_text STRING
                COMMENT 'The text for the level 2 node',
            level_2_node_sort_order STRING
                COMMENT 'The sort order for the level 2 node',
            level_3_node STRING
                COMMENT 'The level 3 node',
            level_3_node_text STRING
                COMMENT 'The text for the level 3 node',
            level_3_node_sort_order STRING
                COMMENT 'The sort order for the level 3 node',
            level_4_node STRING
                COMMENT 'The level 4 node',
            level_4_node_text STRING
                COMMENT 'The text for the level 4 node',
            level_4_node_sort_order STRING
                COMMENT 'The sort order for the level 4 node',
            level_5_node STRING
                COMMENT 'The level 5 node',
            level_5_node_text STRING
                COMMENT 'The text for the level 5 node',
            level_5_node_sort_order STRING
                COMMENT 'The sort order for the level 5 node',
            level_6_node STRING
                COMMENT 'The level 6 node',
            level_6_node_text STRING
                COMMENT 'The text for the level 6 node',
            level_6_node_sort_order STRING
                COMMENT 'The sort order for the level 6 node',
            level_7_node STRING
                COMMENT 'The level 7 node',
            level_7_node_text STRING
                COMMENT 'The text for the level 7 node',
            level_7_node_sort_order STRING
                COMMENT 'The sort order for the level 7 node',
            level_8_node STRING
                COMMENT 'The level 8 node',
            level_8_node_text STRING
                COMMENT 'The text for the level 8 node',
            level_8_node_sort_order STRING
                COMMENT 'The sort order for the level 8 node',
            level_9_node STRING
                COMMENT 'The level 9 node',
            level_9_node_text STRING
                COMMENT 'The text for the level 9 node',
            level_9_node_sort_order STRING
                COMMENT 'The sort order for the level 9 node',
            
            __etl_keys_fprint BIGINT
            COMMENT 'xxhash64 of the Business Keys that this record is made up of (or, where the table is the result of joins, it is the fields that make the record unique)',
        __etl_row_fprint BIGINT
            COMMENT ' the xxhash64 of all the columns that make up the row payload (i.e. all the non-key, and non-metadata columns). Note: all columns must be NOT NULL for the hash to calculate properly.',
        __etl_effective_from DATE
            COMMENT 'date (as DATE datatype) that row is effective from. For an updated record this is the previous _effective_to date + 1 day.',
        __etl_effective_to DATE
            COMMENT 'date (as DATE datatype) that row is effective to, or NULL for active record',
        __etl_is_active BOOLEAN
            COMMENT 'boolean flag indicating the active record. Note: there should only be 1 _is_active for any _etl_keys_fprint',
        __etl_is_deleted BOOLEAN
            COMMENT 'boolean showing if the record has been deleted from the source system'
    )
    CLUSTER BY 
        AUTO;
''')