# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.silver_catalog}.fin_general_ledger.gl_account_master (
        -- keys
        gl_account STRING COMMENT "G/L Account",

        -- payload
        gl_account_description STRING COMMENT "G/L Account Description",
        functional_area STRING COMMENT "Functional Area",
        group_account_number STRING COMMENT "Group Account Number",
        -- rename to "is_balance_sheet_account"?
        balance_sheet_account_flag BOOLEAN COMMENT "Balance sheet account",
        gl_account_type STRING COMMENT "G/L Account Type",
        gl_account_subtype STRING COMMENT "G/L Account Subtype",
        client STRING COMMENT "Client",
        chart_of_accounts STRING COMMENT "Chart of Accounts",
        last_changed_timestamp TIMESTAMP COMMENT "Time Stamp",
        government_receivable_flag BOOLEAN COMMENT "Is the account a government receivable account?",

        -- metadata
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