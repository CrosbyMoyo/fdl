# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.gold_catalog}.fin_general_ledger.dim_gl_account
    (
        gl_account_skey BIGINT
            COMMENT 'G/L account skey',
        chart_of_accounts STRING
            COMMENT 'Chart of accounts',
        gl_account STRING
            COMMENT 'G/L account',
        gl_account_description STRING
            COMMENT 'G/L account description',
        balance_sheet_account_flag BOOLEAN
            COMMENT 'Balance sheet account flag',
        group_account_number STRING
            COMMENT 'Group account number',
        functional_area STRING
            COMMENT 'Functional area',
        gl_account_type STRING
            COMMENT 'G/L account type',
        gl_account_subtype STRING
            COMMENT 'G/L account subtype',
        last_changed_timestamp TIMESTAMP
            COMMENT 'Last changed timestamp',

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
        COMMENT "showing if the record has been deleted from the source system", 
    CONSTRAINT pk_dim_gl_account PRIMARY KEY (gl_account_skey)
    )
    CLUSTER BY
        AUTO;
''')