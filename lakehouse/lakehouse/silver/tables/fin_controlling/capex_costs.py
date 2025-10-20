# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.silver_catalog}.fin_controlling.capex_costs (

        -- keys
        order_number STRING
            COMMENT 'Order Number'

        -- payload
        ,order_type STRING
            COMMENT 'Order Type'
        ,order_category INT
            COMMENT 'Order Category'
        ,created_on DATE
            COMMENT 'Order Creation Date'
        ,description STRING
            COMMENT 'Order Description'
        ,processing_group INT
            COMMENT 'Processing Group'
        ,subtype_name STRING 
            COMMENT 'Order Subtype'
        ,company_code STRING
            COMMENT 'Company Code'
        ,plant STRING
            COMMENT 'Plant where the order is being processed'
        ,phase0_order_created STRING
            COMMENT 'order created flag'
        ,phase1_order_released STRING
            COMMENT 'order released flag'
        ,controlling_area STRING
            COMMENT 'Controlling Area'
        ,phase2_order_completed STRING
            COMMENT 'order completed flag'
        ,phase3_order_closed STRING
            COMMENT 'order closed flag'
        ,order_currency STRING
            COMMENT 'Order Currency'
        ,object_number STRING
            COMMENT 'Object Number'
        ,profit_center STRING
            COMMENT 'Profit Center'
        ,ledger STRING
            COMMENT 'budget/planning Ledger'
        ,fiscal_year INT
            COMMENT 'Fiscal Year'
        ,budget_local DECIMAL(18,2)
            COMMENT 'Budget Amount in Local Currency'
        ,allocated_local DECIMAL(18,2)
            COMMENT 'Allocated Amount in Local Currency'
        ,budget_usd DECIMAL(18,2)
            COMMENT 'Budget Amount in USD'
        ,actuals_usd DECIMAL(18,2)
            COMMENT 'Actuals Amount in USD'
        ,committed_usd DECIMAL(18,2)
            COMMENT 'Committed Amount in USD'
        ,actuals_local DECIMAL(18,2)
            COMMENT 'Actuals Amount in Local Currency'
        ,committed_local DECIMAL(18,2)
            COMMENT 'Committed Amount in Local Currency'
        
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