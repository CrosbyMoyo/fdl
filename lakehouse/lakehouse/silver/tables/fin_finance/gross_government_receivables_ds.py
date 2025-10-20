# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.silver_catalog}.fin_finance.gross_government_receivables_ds (
        -- KEYS
        company_code STRING
            COMMENT 'Code identifying the company',
        company_code_currency STRING
            COMMENT 'company code currency',
        fiscal_year STRING
            COMMENT 'fiscal year',
        posting_period INT
            COMMENT 'the period for posting',
        exchange_rate DECIMAL(8,2)
            COMMENT 'Exchange rate',

        -- PAYLOAD
        offset_value DECIMAL(38,2)
            COMMENT 'offset values', 
        payments DECIMAL(38,2) 
            COMMENT 'payments', 
        amount_gbr_usd DECIMAL(38,2)
            COMMENT 'amount gbr usd value', 
        amount_local_currency DECIMAL(38,2)
            COMMENT 'amount local currency', 
        opening_balance DECIMAL(38,2)
            COMMENT 'Opening balance',

        -- Keys
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
    CLUSTER BY AUTO;
''')