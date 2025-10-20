# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.silver_catalog}.fin_controlling.cost_element_hierarchy (
        -- Keys 
        hierarchy_id STRING
            COMMENT "Hierarchy ID",
        cost_element_hierarchy_node STRING
            COMMENT "The profit center hierarchy node ID. This is the node ID at the root of the hierarchy",
        -- Payload
        controlling_area STRING
            COMMENT "Controlling area",
        hierarchy_name STRING
            COMMENT "Hierarchy Name",
        hierarchy_level STRING
            COMMENT "The level in the hierarchy",
        is_leaf_node BOOLEAN
            COMMENT "Flag to indicate if this is a leaf node or not",
        level_1_node STRING
            COMMENT "The ID of the level 1 node",
        level_1_node_text STRING
            COMMENT "The text description of the level 1 node",
        level_1_node_sort_order STRING
            COMMENT "The sort order for the level 1 node",
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

        -- ETL Fields 
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