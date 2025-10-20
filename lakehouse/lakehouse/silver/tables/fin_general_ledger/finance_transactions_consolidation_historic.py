# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.silver_catalog}.fin_general_ledger.finance_transactions_consolidation_historic (
        -- New Columns
        actual_plan_code STRING COMMENT 'For actual data this is the string literal "Actual", and for plan data this is the plan code',
        datasource STRING COMMENT 'Datasource',
        journal_type STRING COMMENT 'Journal Type',
        posting_date DATE COMMENT 'Date',
        gl_account STRING COMMENT 'GL Account',
        company_code STRING COMMENT 'Company Code',
        controlling_area STRING COMMENT 'Controlling Area',
        currency_key STRING COMMENT 'Currency Key',
        profit_center STRING COMMENT 'Profit Center',
        consolidation_record_type STRING COMMENT 'Consolidation Record Type',
        cost_center STRING COMMENT 'Cost Center',
        vcode STRING COMMENT 'V Code',
        fiscal_year_period STRING COMMENT 'Fiscal Year Period',
        posting_period STRING COMMENT 'Posting Period',
        fiscal_year STRING COMMENT 'Fiscal Year',
        vcode_amount_local DECIMAL(26,8) COMMENT 'V Code Amount Local',
        amount_local_currency DECIMAL(26,8) COMMENT 'Amount Local Currency',
        quantity DECIMAL(26,8) COMMENT 'Quantity',
        volume_kg DECIMAL(26,8) COMMENT 'Volume KG',
        volume_litres_l20 DECIMAL(26,8) COMMENT 'Volume Litres 120',

        -- Metadata
        __etl_keys_fprint BIGINT
            COMMENT 'xxhash64 of the Business Keys that this record is made up of (or, where the table is the result of joins, it is the fields that make the record unique)',
        __etl_row_fprint BIGINT
            COMMENT ' the xxhash64 of all the columns that make up the row payload (i.e. all the non-key, and non-metadata columns). Note: all columns must be NOT NULL for the hash to calculate properly.',
        __etl_effective_from DATE
            COMMENT 'DATE (as DATE datatype) that row is effective from. For an updated record this is the previous _effective_to DATE + 1 day.',
        __etl_effective_to DATE
            COMMENT 'DATE (as DATE datatype) that row is effective to, or NULL for active record',
        __etl_is_active BOOLEAN
            COMMENT 'boolean flag indicating the active record. Note: there should only be 1 _is_active for any _etl_keys_fprint',
        __etl_is_deleted BOOLEAN
            COMMENT 'boolean showing if the record has been deleted from the source system'
    ) 
    CLUSTER BY AUTO;
''')

# COMMAND ----------

