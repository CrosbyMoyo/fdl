# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.gold_catalog}.fin_general_ledger.dim_profit_loss_subcategory (
        -- Keys
        vcode STRING
            COMMENT 'Vcode',
        parent STRING
            COMMENT 'Parent',
        master_parent STRING
            COMMENT 'Master Parent',

        -- Payload
        sub_category STRING
            COMMENT 'Sub category',
        parent_category STRING
            COMMENT 'Parent Category',
        master_parent_category STRING
            COMMENT 'Master Parent Category',

        -- Metadata columns
        __etl_keys_fprint BIGINT
        COMMENT "xxhash64 of the Business Keys that this record is made up of",
        __etl_row_fprint BIGINT
        COMMENT "the xxhash64 of all the columns that make up the row payload",
        __etl_effective_from DATE
        COMMENT "Date that row is effective from",
        __etl_effective_to DATE
        COMMENT "Date that row is effective to, or NULL for active record",
        __etl_is_active BOOLEAN
        COMMENT "flag indicating the active record. Note: there should only be 1 _is_active for any _etl_keys_fprint",
        __etl_is_deleted BOOLEAN
        COMMENT "showing if the record has been deleted from the source system"
    )
    CLUSTER BY
        AUTO;
''')