# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.gold_catalog}.fin_controlling.fact_special_items
    (
        -- FKs
        year_month STRING
            COMMENT 'FK to dim_date year_month',

        -- PAYLOAD
        special_item_code STRING 
            COMMENT 'special items code values',
        amount_mtd DECIMAL(8,2)
            COMMENT 'amount mtd',
        amount_ytd DECIMAL(8,2)
            COMMENT 'amount ytd',

        -- metadata
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