# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.silver_catalog}.fin_general_ledger.sgr_manual_adjustments (
        --key
        fiscal_year INT
            COMMENT 'Fiscal Year',
        dimension STRING
            COMMENT 'dimension',
        ledger STRING
            COMMENT 'ledger',
        
        -- payload
        subitem_category STRING
            COMMENT 'subitem category',
        subitem STRING
            COMMENT 'subitem',
        posting_level STRING
            COMMENT 'posting level',
        document_type STRING
            COMMENT 'document type',
        amount_in_local_currency DECIMAL(23,4)
            COMMENT 'amount in local currency',
        date_created DATE
            COMMENT 'date created',
        chart_of_accounts STRING
            COMMENT 'chart of accounts',
        quantity DECIMAL(23,4)
            COMMENT 'quantity',
        gl_account STRING
            COMMENT 'gl account',
        base_unit_of_measure STRING
            COMMENT 'base unit of measure',
        material_number STRING
            COMMENT 'material',
        fiscal_year_period INT
            COMMENT 'fiscal year period',
        consolidation_chart_of_accounts STRING
            COMMENT 'consolidation chart of accounts',
        cost_center STRING
            COMMENT 'cost center',
        profit_center STRING
            COMMENT 'profit center',
        controlling_area STRING
            COMMENT 'controlling area',
        record_type STRING
            COMMENT 'record type',
        consolidation_version STRING
            COMMENT 'consolidation version',
        local_currency STRING
            COMMENT 'local currency',
        posting_period INT
            COMMENT 'posting period',
        document_category STRING
            COMMENT 'document category',
        consolidation_unit STRING
            COMMENT 'consolidation unit',
        financial_statement_item STRING
            COMMENT 'financial statement item',

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