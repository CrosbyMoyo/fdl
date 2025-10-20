# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.silver_catalog}.fin_finance.sgr_transactions_old_new_report_logic (
        -- Keys
        client STRING
            COMMENT 'Client identifier',
        company_code STRING
            COMMENT 'Company identifier',
        financial_statement_item STRING
            COMMENT 'Item in the financial statement',
        consolidation_unit STRING
            COMMENT 'Consolidation unit',
        profit_center STRING
            COMMENT 'Profit center',
        segment STRING
            COMMENT 'Segment',
        consolidation_group STRING 
            COMMENT 'Consolidation group',
        consolidation_document_type STRING
            COMMENT 'Type of consolidation document', 
        posting_level STRING
            COMMENT 'Posting level',
        consolidation_version STRING
            COMMENT 'Version of consolidation',
        fiscal_year INT
            COMMENT 'Fiscal year',
        fiscal_period INT
            COMMENT 'Fiscal period',
        fiscal_year_period STRING
            COMMENT 'Fiscal year period',
        creation_date STRING
            COMMENT 'Creation date',
        month_end_date DATE
            COMMENT 'Month end date',

        -- Payload
        group_currency STRING
            COMMENT 'Group currency', 
        local_currency STRING
            COMMENT 'Local currency',
        amount_in_local_currency DECIMAL(28,8)
            COMMENT 'Amount in local currency',
        amount_in_group_currency DECIMAL(28,8)
            COMMENT 'Amount in group currency',
        consolidation_document_number STRING
            COMMENT 'Number of consolidation document',
        consolidation_posting_item STRING
            COMMENT 'Item in consolidation posting',
        chart_of_accounts STRING
            COMMENT 'Chart of accounts',
        gl_account STRING
            COMMENT 'General ledger account',
        period_mode STRING
            COMMENT 'Period mode',
        consolidation_chart_of_accounts STRING
            COMMENT 'Consolidation chart of accounts',
        controlling_area STRING
            COMMENT 'Consolidation chart of accounts',
        cost_center STRING
            COMMENT 'Consolidation chart of accounts',
        consolidation_ledger STRING
            COMMENT 'Consolidation chart of accounts',

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