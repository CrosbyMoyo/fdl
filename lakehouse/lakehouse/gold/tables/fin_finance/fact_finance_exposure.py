# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.gold_catalog}.fin_finance.fact_finance_exposure
    (
        --PK
        fact_finance_exposure_skey BIGINT
            CONSTRAINT fact_finance_exposure__pk PRIMARY KEY
            COMMENT 'Surrogate Key for the fact table',

        -- FKs
        snapshot_date_key DATE
            CONSTRAINT fact_finance_exposure_dim_date_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.ca_cross_application_components.dim_date(date_key)
            COMMENT 'FK to dim_date',
        company_code_skey BIGINT
            CONSTRAINT fact_finance_exposure_dim_company_code_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code(company_code_skey)
            COMMENT 'FK to dim_company_code',
        datasource_skey BIGINT
            CONSTRAINT fact_finance_exposure_dim_datasource_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.ca_cross_application_components.dim_datasource(datasource_skey)
            COMMENT 'FK to dim_datasource',

        -- measures
        goods_received_not_invoiced_in_group_currency DECIMAL(23,2)
            COMMENT 'Goods received not invoiced in group currency',
        accounts_payable_in_group_currency DECIMAL(23,2)
            COMMENT 'Accounts payable in group currency',
        accounts_receivable_group_currency DECIMAL(23,2)
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
        accounts_receivable_in_local_currency DECIMAL(33,2)
            COMMENT 'Accounts receivable in local currency',
        imports_in_local_currency DECIMAL(18,2)
            COMMENT 'Imports in local currency',
        exports_in_local_currency DECIMAL(18,2)
            COMMENT 'Exports in local currency',
        accounts_payable_in_local_currency DECIMAL(33,2)
            COMMENT 'Accounts payable in local currency',
        overdraft_balance DECIMAL(17,2)
            COMMENT 'Overdraft balance',
        document_currency STRING
            COMMENT 'Document currency',
        dividends_payable DECIMAL(8,2)
            COMMENT 'Dividends payable',
        product_price_not_delivered DECIMAL(8,2)
            COMMENT 'Product price not delivered',

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