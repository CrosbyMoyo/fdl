# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.gold_catalog}.fin_finance.fact_sgr_transaction_summary
    (   
        --PK
        fact_sgr_transaction_summary_skey BIGINT
            CONSTRAINT fact_sgr_transaction_summary__pk PRIMARY KEY
            COMMENT 'Surrogate Key to the fact table',

        datasource STRING
            COMMENT 'Silver datasource',
        -- FKs
        company_code_skey BIGINT
            CONSTRAINT sgr_transaction_summary__dim_company_code_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.cal_cross_application_components.dim_company_code(company_code_skey)
            COMMENT 'FK to dim_company_code',
        date_key DATE
            CONSTRAINT sgr_transaction_summary__dim_date_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.ca_cross_application_components.dim_date(date_key)
            COMMENT 'FK to dim_date',
        profit_center_skey BIGINT
            CONSTRAINT sgr_transaction_summary__dim_profit_center_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.fin_controlling.dim_profit_center(profit_center_skey)
            COMMENT 'FK to dim_profit_center',
        company_code_skey BIGINT
            CONSTRAINT sgr_transaction_summary__dim_company_code_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code(company_code_skey)
            COMMENT 'FK to dim_company_code',
        gl_account_skey BIGINT
            CONSTRAINT sgr_transaction_summary__dim_gl_account_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.fin_general_ledger.dim_gl_account(gl_account_skey)
            COMMENT 'FK to dim_gl_account',
        financial_statement_item_skey BIGINT
            CONSTRAINT sgr_transaction_summary__dim_financial_statement_item_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.fin_general_ledger.dim_financial_statement_item_hierarchy(financial_statement_item_skey)
            COMMENT 'FK to dim_financial_statement_item',
        consolidation_unit_skey BIGINT
            CONSTRAINT sgr_transaction_summary__dim_consolidation_unit_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.fin_general_ledger.dim_consolidation_unit(consolidation_unit_skey)
            COMMENT 'FK to dim_consolidation_unit',
        consolidation_reporting_item_skey BIGINT
            CONSTRAINT sgr_transaction_summary__dim_consolidation_reporting_item_skey
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.fin_general_ledger.dim_consolidation_reporting_item_hierarchy(consolidation_reporting_item_skey)
            COMMENT 'FK to dim_consolidation_reporting_item_hierarchy',
        consolidation_segment_skey BIGINT
            CONSTRAINT sgr_transaction_summary__dim_consolidation_segment_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.fin_general_ledger.dim_consolidation_segment(consolidation_segment_skey)
            COMMENT 'FK to dim_consolidation_segment',
        posting_level_skey BIGINT
            CONSTRAINT sgr_transaction_summary__dim_posting_level_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.fin_general_ledger.dim_posting_level_hierarchy(posting_level_skey)
            COMMENT 'FK to dim_posting_level',
        --
        local_currency_skey STRING
            COMMENT 'FK to dim_currency (coming soon)',
        group_currency_skey STRING
            COMMENT 'FK to dim_currency (coming soon)',
        period_mode STRING
            COMMENT 'Period Mode',
        consolidation_version STRING
            COMMENT 'Consolidation Version',
        consolidation_document_type STRING
            COMMENT 'Consolidation Document Type',
        fiscal_year INT
            COMMENT 'Fiscal Year',
        fiscal_period INT
            COMMENT 'Fiscal Period',

        -- Measures
        amount_local_currency DECIMAL(28,8)
            COMMENT 'Local (OU) currency amount',
        amount_group_currency DECIMAL(28,8)
            COMMENT 'Group Currency (USD) amount',

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