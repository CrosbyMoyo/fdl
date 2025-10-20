# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.silver_catalog}.fin_finance.finance_exposure (
        -- KEYS
        company_code STRING
            COMMENT 'Code identifying the company',
        snapshot_date DATE
            COMMENT 'Snapshot date',
        document_currency STRING
            COMMENT 'Document currency',
        source_system STRING
            COMMENT 'Source system',

        -- PAYLOAD
        goods_received_not_invoiced_in_group_currency DECIMAL(23,2)
            COMMENT 'Goods received not invoiced in group currency', 
        accounts_payable_in_group_currency DECIMAL(23,2)
            COMMENT 'Accounts payable in group currency', 
        accounts_receivable_group_currency DECIMAL(33,2)
            COMMENT 'Accounts receivable group currency', 
        general_ledger_borrowings_group_currency DECIMAL(23,2)
            COMMENT 'General ledger borrowings group currency', 
        imports DECIMAL(5,2)
            COMMENT 'Imports',
        exports DECIMAL(5,2)
            COMMENT 'Exports',
        reporting_currency STRING
            COMMENT 'Reporting currency',
        cash_balance DECIMAL(17,2)
            COMMENT 'Cash balance',
        goods_received_not_invoiced_in_local_currency DECIMAL(33,2)
            COMMENT 'Goods received not invoiced in local currency',
        accounts_receivable_in_local_currency  DECIMAL(33,2)
            COMMENT 'Accounts receivable in local currency',
        imports_in_local_currency DECIMAL(18,2)
            COMMENT 'Imports in local currency',
        exports_in_local_currency DECIMAL(18,2)
            COMMENT 'Exports in local currency',
        accounts_payable_in_local_currency DECIMAL(33,2)
            COMMENT 'Accounts payable in local currency',
        overdraft_balance DECIMAL(17,2)
            COMMENT 'Overdraft balance',
        reporting_week INT
            COMMENT 'Reporting week',
        dividends_payable DECIMAL(8,2)
            COMMENT 'Dividends payable',
        product_price_not_delivered DECIMAL(8,2)
            COMMENT 'Product price not delivered',

        -- Keys
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