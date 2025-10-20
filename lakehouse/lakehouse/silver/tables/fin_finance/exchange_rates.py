# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.silver_catalog}.fin_finance.exchange_rates (
        -- Key
        valid_from DATE
            COMMENT 'Date from which the entry is valid. Queries should always look at the latest',
        from_currency STRING
            COMMENT 'From currency code',
        to_currency STRING
            COMMENT 'To currency code',
        exchange_rate_type STRING
            COMMENT 'Exchange rate type for currency translation',
        inverted_rate BOOLEAN
            COMMENT 'Boolean flag indicating whether the exchange rate was calculated from the inverse conversion',
            
        -- Payload
        scaled_exchange_rate DECIMAL(26, 21)
            COMMENT 'Exchange rate with decimal and factor scaling applied',
        -- Metadata
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