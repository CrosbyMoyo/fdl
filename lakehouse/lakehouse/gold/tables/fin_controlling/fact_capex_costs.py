# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.gold_catalog}.fin_controlling.fact_capex_costs
    (
        -- FKs
        internal_order STRING
            COMMENT 'PK for the fact table',
        date_key DATE
            CONSTRAINT fact_capex_costs_dim_date_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.ca_cross_application_components.dim_date(date_key)
            COMMENT 'FK to dim_date',
        company_code_skey  BIGINT
            CONSTRAINT fact_capex_costs_dim_company_code_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code(company_code_skey)
            COMMENT 'FK to dim_company_code',
        subtype_name_skey  BIGINT
            CONSTRAINT fact_capex_costs_dim_capex_subtype_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.fin_finance.dim_capex_subtype(subtype_name_skey)
            COMMENT 'FK to dim_capex_subtype',
        profit_center_skey BIGINT
            CONSTRAINT fact_capex_costs_dim_profit_center_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.fin_controlling.dim_profit_center(profit_center_skey)
            COMMENT 'FK to dim_profit_center',
        line_of_business_skey  BIGINT
            CONSTRAINT fact_capex_costs_dim_line_of_business_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.fin_controlling.dim_line_of_business(line_of_business_skey)
            COMMENT 'FK to dim_line_of_business',
        currency_skey  STRING
            COMMENT 'currency key',
        plant_skey STRING
            COMMENT 'plant',

        -- PAYLOAD
        object_number STRING
            COMMENT 'object number',
        order_type STRING
            COMMENT 'order type',
        order_category STRING
            COMMENT 'order category',
        ledger STRING
            COMMENT 'ledger',
        fiscal_year INT
            COMMENT 'fiscal year',

        -- measures
        budget_local DECIMAL(18,2)
            COMMENT 'budget local',
        allocated_local DECIMAL(18,2)
            COMMENT 'allocated local',
        budget_usd DECIMAL(18,2)
            COMMENT 'budget usd',
        actuals_usd DECIMAL(18,2)
            COMMENT 'actuals usd',
        committed_usd DECIMAL(18,2)
            COMMENT 'committed usd',
        actuals_local DECIMAL(18,2)
            COMMENT 'actuals local',
        committed_local DECIMAL(18,2)
            COMMENT 'committed local',

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