# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.silver_catalog}.fin_controlling.order_master_data (
        -- KEYS
        order_number STRING
            COMMENT "Unique identifier for each order"
        ,client STRING
            COMMENT "Client associated with the order"

        -- PAYLOAD
        ,plant STRING
            COMMENT "Plant where the order is being processed"
        ,order_status INT  
            COMMENT "Order status"
        ,created_on DATE
            COMMENT "Date the order was created"
        ,description STRING
            COMMENT "Description of the order"
        ,phase3_order_closed STRING
            COMMENT "order closed flag"
        ,company_code STRING
            COMMENT "Code identifying the company related to the order"
        ,order_type STRING
            COMMENT "Order type"
        ,phase0_order_created STRING
            COMMENT "order created flag"
        ,order_category INT
            COMMENT "Order category"
        ,controlling_area STRING
            COMMENT "Area responsible for controlling the order"
        ,profit_center STRING
            COMMENT "Profit center associated with the order"
        ,order_currency STRING
            COMMENT "Currency associated with the order"
        ,object_number STRING
            COMMENT "Object number associated with the order"
        ,phase1_order_released STRING
            COMMENT "order released flag"
        ,processing_group INT
            COMMENT "Processing group"
        ,phase2_order_completed STRING
            COMMENT "order completed flag"
        ,reference_order STRING
            COMMENT "Reference order"
 
        -- metadata
        ,__etl_keys_fprint BIGINT
            COMMENT 'xxhash64 of the Business Keys that this record is made up of (or, where the table is the result of joins, it is the fields that make the record unique)'
        ,__etl_row_fprint BIGINT
            COMMENT ' the xxhash64 of all the columns that make up the row payload (i.e. all the non-key, and non-metadata columns). Note: all columns must be NOT NULL for the hash to calculate properly.'
        ,__etl_effective_from DATE
            COMMENT 'date (as DATE datatype) that row is effective from. For an updated record this is the previous _effective_to date + 1 day.'
        ,__etl_effective_to DATE
            COMMENT 'date (as DATE datatype) that row is effective to, or NULL for active record'
        ,__etl_is_active BOOLEAN
            COMMENT 'boolean flag indicating the active record. Note: there should only be 1 _is_active for any _etl_keys_fprint'
        ,__etl_is_deleted BOOLEAN
            COMMENT 'boolean showing if the record has been deleted from the source system'
        )
        CLUSTER BY AUTO;
''')