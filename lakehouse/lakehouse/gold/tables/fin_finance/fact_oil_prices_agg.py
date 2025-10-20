# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.gold_catalog}.fin_finance.fact_oil_prices_agg
    (
        --PK
        fact_oil_prices_agg_skey BIGINT
            COMMENT 'Surrogate Key for the fact table',
        -- FKs
        year_month STRING
            COMMENT 'FK to dim_date',

        -- Payload
        symbol STRING
            COMMENT 'Symbol',
        avg_price DECIMAL(38,2)
            COMMENT 'Average base oil price',

        -- Metadata
        __etl_fprint BIGINT
            COMMENT 'xxhash64 of the columns that make up this row: FKs and payload combined',
        __etl_load_timestamp TIMESTAMP
            COMMENT 'datetime that the row was added to the table',
        __etl_is_active BOOLEAN
            COMMENT 'flag indicating the active record. Note: there should only be 1 _is_active for any _etl_fprint',
        __etl_is_deleted BOOLEAN
            COMMENT 'flag showing if the record has been deleted from the source system'

    )
    CLUSTER BY AUTO;

''')