# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.silver_catalog}.fin_controlling.totals_for_budget_and_plan (

        -- keys
        year_of_cash_effectivity INT
            COMMENT 'Year of Cash Effectivity'
        ,budget_subtype STRING
            COMMENT 'The budget subtype'
        ,ledger STRING
            COMMENT 'Budget/Planning Ledger'
        ,client STRING
            COMMENT 'client'
        ,fund STRING
            COMMENT 'fund'
        ,planning_budget_version STRING
            COMMENT 'Planning/budgeting version'
        ,value_type STRING
            COMMENT 'The value type'
        ,budget_type STRING
            COMMENT 'Budget Type Budgeting/Planning'
        ,object_indicator STRING
            COMMENT 'object indicator'
        ,internal_cmmt_item_8 STRING
            COMMENT 'Internal commitment item (8 chars)'
        ,fiscal_year INT
            COMMENT 'fiscal year'
        ,object_number STRING
            COMMENT 'object number'  
        ,functional_area STRING  
            COMMENT 'The functional area' 
        ,transaction_currency STRING
            COMMENT 'The transaction currency'

        -- payload
        ,annual_value_transaction_currency DECIMAL(15,2)
            COMMENT 'The annual value in the transaction currency'
        ,distributed_annual_value_ledger_currency DECIMAL(15,2)
            COMMENT 'The distributed annual value in the ledger currency'
        ,distributed_annual_value_transaction_currency DECIMAL(15,2)
            COMMENT 'The distributed annual value in the transaction currency'
        ,annual_value_ledger_currency DECIMAL(15,2)
            COMMENT 'The annual value in the ledger currency'

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