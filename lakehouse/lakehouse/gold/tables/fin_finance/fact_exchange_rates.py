# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.gold_catalog}.fin_finance.fact_exchange_rates
    (
        fact_exchange_rates_skey BIGINT
            CONSTRAINT fact_exchange_rates__pk PRIMARY KEY
            COMMENT 'Surrogate Key for the fact table',

        -- FKs
        valid_from_key DATE
            CONSTRAINT fact_exchange_rates__dim_date__fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.ca_cross_application_components.dim_date(date_key)
            COMMENT 'Date as of which the exchange rate is effective. FK to dim_date',

        -- Payload
        exchange_rate_type STRING
            COMMENT 'The exchange rate type',
        from_currency STRING
            COMMENT 'From currency',
        to_currency STRING
            COMMENT 'To currency',
        scaled_exchange_rate DECIMAL(26, 21)
            COMMENT 'Exchange rate with decimal and factor scaling applied',

        -- Metadata
        __etl_fprint BIGINT
            COMMENT ' the xxhash64 of all the columns that make up the row payload (i.e. all the non-key, and non-metadata columns). Note: all columns must be NOT NULL for the hash to calculate properly.',
        __etl_load_timestamp DATE
            COMMENT 'date (as DATE datatype) that row is effective from. For an updated record this is the previous _effective_to date + 1 day.',
        __etl_is_active BOOLEAN
            COMMENT 'boolean flag indicating the active record. Note: there should only be 1 _is_active for any _etl_keys_fprint',
        __etl_is_deleted BOOLEAN
            COMMENT 'boolean showing if the record has been deleted from the source system'
    )
    CLUSTER BY
        AUTO;
''')