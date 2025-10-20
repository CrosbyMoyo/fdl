# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.gold_catalog}.fin_finance.fact_gross_government_receivables
     (
        -- Keys
        fact_gross_government_receivables_skey BIGINT COMMENT 'Surrogate Key for the fact table',
        company_code STRING COMMENT 'Company Code',
        fiscal_year INT COMMENT 'Fiscal Year',
        fiscal_year_period INT COMMENT 'Fiscal Year Period',
        year_month STRING COMMENT 'Year Month',
        posting_period STRING COMMENT 'Posting Period',
        currency_key STRING COMMENT 'Company Code Currency',
        global_currency STRING COMMENT 'Global Currency',

        -- Payload
        ytd_amounts_in_company_code_currency STRING COMMENT 'Year-to-date amounts in company code currency',
        ytd_offsets_in_company_code_currency STRING COMMENT 'Year-to-date offsets in company code currency',
        ytd_payments_in_company_code_currency STRING COMMENT 'Year-to-date payments in company code currency',
        ytd_amounts_in_global_currency INT COMMENT 'Year-to-date amounts in global currency',
        ytd_offsets_in_global_currency STRING COMMENT 'Year-to-date offsets in global currency',
        ytd_payments_in_global_currency STRING COMMENT 'Year-to-date payments in global currency',
        

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