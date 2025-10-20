# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.gold_catalog}.fin_finance.fact_finance_transaction_details
    (
        fact_finance_transaction_details_skey BIGINT
            CONSTRAINT fact_finance_transaction_details__pk PRIMARY KEY
            COMMENT 'Surrogate Key for the fact table',

        -- FKs
        date_key DATE
            CONSTRAINT fact_finance_transaction_details__dim_date_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.ca_cross_application_components.dim_date(date_key)
            COMMENT 'FK to dim_date',
        profit_center_skey BIGINT
            CONSTRAINT fact_finance_transaction_details__dim_profit_center_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.fin_controlling.dim_profit_center(profit_center_skey)
            COMMENT 'FK to dim_profit_center',
        line_of_business_skey BIGINT
            CONSTRAINT fact_finance_transaction_details__dim_line_of_business_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.fin_controlling.dim_line_of_business(line_of_business_skey)
            COMMENT 'FK to dim_line_of_business',
        company_code_skey BIGINT
            CONSTRAINT fact_finance_transaction_details__dim_company_code_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code(company_code_skey)
            COMMENT 'FK to dim_company_code',
        country_key STRING
            CONSTRAINT fact_finance_transaction_details__dim_country_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.ca_cross_application_components.dim_country(country_key)
            COMMENT 'FK to dim_country',
        vcode_skey BIGINT
            CONSTRAINT fact_finance_transaction_details__dim_vcode_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.fin_general_ledger.dim_vcodes(vcode_skey)
            COMMENT 'FK to dim_vcode',
        cost_center_skey BIGINT
            CONSTRAINT fact_finance_transaction_details__dim_cost_center_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.fin_controlling.dim_cost_center(cost_center_skey)
            COMMENT 'FK to dim_cost_center',
        gl_account_skey BIGINT
            CONSTRAINT fact_finance_transaction_details__dim_gl_account_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.fin_general_ledger.dim_gl_account(gl_account_skey)
            COMMENT 'FK to dim_gl_account',
        datasource_skey BIGINT
            CONSTRAINT fact_finance_transaction_details__dim_datasource_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.ca_cross_application_components.dim_datasource(datasource_skey)
            COMMENT 'FK to dim_datasource',
        material_skey BIGINT
            CONSTRAINT fact_finance_transaction_details__dim_material_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.sl_extended_warehouse_management.dim_material(material_skey)
            COMMENT 'FK to dim_material',

        --
        local_currency_skey BIGINT
            COMMENT 'FK to dim_currency (coming soon)',
        group_currency_skey BIGINT
            COMMENT 'FK to dim_currency (coming soon)',
        elimination_flag STRING
            COMMENT 'Elimination Flag for Indication Reports',
        actual_plan_code STRING
            COMMENT 'For actual data this is the string literal "Actual", and for plan data this is the plan code',
        --

        -- measures
        amount_local_currency DECIMAL(18,4)
            COMMENT 'Local (OU) currency amount',
        local_currency_code STRING
            COMMENT 'The ISO-4217 currency code', -- delete
        amount_group_currency DECIMAL(28,8)
            COMMENT 'Group Currency (USD) amount',
        amount_group_currency_plan_rate DECIMAL(28,8)
            COMMENT 'Group currency amount using planning exchange rate',
        amount_group_currency_month_end DECIMAL(28,8)
            COMMENT 'Group currency amount using the current year exchange rate (Vivo Monthly Average) to remove currency fluctuation affects for analysise',
        amount_group_currency_plus_1_year_rate DECIMAL(28,8)
            COMMENT 'Group currency rate for 1 year ahead applied to this month',
        volume_litres_l20 DECIMAL(18,4)
            COMMENT 'Volume in Litres at 20 degrees',
        volume_kg DECIMAL(18,4)
            COMMENT 'Volume in KG',
        volume_issued_litres_l20 DECIMAL(18,4)
            COMMENT 'Cost of Sales Volume Issued in Litres at 20 Degrees',
        vcode_amount_local DECIMAL(28,8)
            COMMENT 'Vcode Amount Local',
        vcode_amount_group DECIMAL(28,8)
            COMMENT 'Vcode Amount Group',

        -- reconciliation columns
        document_number STRING,
        posting_item STRING,
        document_status STRING,
        document_type STRING,
        customer STRING,
        bill_to_party STRING,
        ship_to_party STRING,
        material STRING,
        ifrs_flag BOOLEAN,

        -- exchange rates
        fx_rate_avg_monthly DECIMAL(18,12)
            COMMENT 'Vivo-Monthly Average Rate',
        fx_rate_month_end DECIMAL(18,12)
            COMMENT 'End Rate Forex revaluation',
        fx_rate_planning DECIMAL(18,12)
            COMMENT 'Standard Translation for Average Rate',
        balance_sheet_account_flag BOOLEAN
            COMMENT 'Is this a P&L or Balance Sheet?  Used in the plus_1_year_rate calculation',

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