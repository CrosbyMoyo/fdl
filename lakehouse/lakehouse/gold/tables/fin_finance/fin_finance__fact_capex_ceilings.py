# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.gold_catalog}.fin_finance.fact_capex_ceilings
    (
        --PK
        fact_capex_ceilings_skey BIGINT
            CONSTRAINT fact_capex_ceilings__pk PRIMARY KEY
            COMMENT 'Surrogate Key to the fact table',

        -- FKs
        budget_year INT
            COMMENT 'year of budget',
        company_code_skey BIGINT
            CONSTRAINT fact_capex_ceiling_dim_company_code_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code(company_code_skey)
            COMMENT 'FK to dim_company_code',
        line_of_business_skey BIGINT
            CONSTRAINT fact_capex_ceiling_dim_line_of_business_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.fin_controlling.dim_line_of_business(line_of_business_skey)
            COMMENT 'FK to dim_line_of_business',
        country_skey STRING
            CONSTRAINT fact_capex_ceiling_dim_country_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.ca_cross_application_components.dim_country(country_key)
            COMMENT 'FK to dim_country',
        subtype_name_skey LONG
            CONSTRAINT fact_capex_ceiling_dim_capex_subtype_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.fin_finance.dim_capex_subtype(subtype_name_skey)
            COMMENT 'FK to dim_capex_subtype',
        currency_skey STRING
            COMMENT 'currency key',        

        -- measures
        ceiling DECIMAL(36,12)
            COMMENT 'ceiling for line of business',

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