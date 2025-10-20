# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.silver_catalog}.fin_finance.sgr_transactions_with_reporting_rules (
        -- Keys
        client STRING
            COMMENT 'Client Identifier',
        company_code STRING
            COMMENT 'Company identifier',
        consolidation_reporting_item STRING
            COMMENT 'Consolidation Reporting Item',
        consolidation_version STRING
            COMMENT 'Consolidation Version',
        fiscal_year_period STRING
            COMMENT 'Fiscal Year Period',
        period_mode STRING
            COMMENT 'Period Mode',
        consolidation_chart_of_accounts STRING 
            COMMENT 'Consolidation Chart Of Accounts',
        consolidation_document_number STRING
            COMMENT 'Consolidation Document Number', 
        consolidation_posting_item STRING
            COMMENT 'Consolidation Posting Item',
        consolidation_group STRING
            COMMENT 'Consolidation Group',
        consolidation_unit STRING
            COMMENT 'Consolidation Unit',

        -- Payload
        gl_account STRING
            COMMENT 'GL Account',
        financial_statement_item STRING
            COMMENT 'Financial Statement Item', 
        profit_center STRING
            COMMENT 'Profit Center',
        segment STRING
            COMMENT 'Segment',
        fiscal_year INT
            COMMENT 'Fiscal Year',
        fiscal_period INT
            COMMENT 'Fiscal Period',
        group_currency STRING
            COMMENT 'Group Currency',
        local_currency STRING
            COMMENT 'Local Currency',
        amount_in_local_currency DECIMAL(28,8)
            COMMENT 'Amount In Local Currency',
        amount_in_group_currency DECIMAL(28,8)
            COMMENT 'Amount In Group Currency',
        debit_credit_code STRING
            COMMENT 'Debit Credit Code',
        posting_level STRING
            COMMENT 'Posting Level',
        chart_of_accounts STRING
            COMMENT 'Chart Of Accounts',
        consolidation_document_type STRING
            COMMENT 'Consolidation Document Type',
        controlling_area STRING
            COMMENT 'Controlling Area',
        creation_date DATE
            COMMENT 'Creation Date',
        month_end_date DATE
            COMMENT 'Month End Date',

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
    CLUSTER BY AUTO;
''')