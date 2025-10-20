# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.gold_catalog}.fin_finance.fact_gross_government_receivables_ds
    (
        -- Keys
        company_code_skey BIGINT
            CONSTRAINT fact_gross_gov_rec_dim_company_code_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code(company_code_skey)
            COMMENT 'FK to dim_company_code',  

        -- Payload
        payments DECIMAL(38,2)
            COMMENT 'payments',
        amount_gbr_usd DECIMAL(38,2)
            COMMENT 'amount in GBR in USD',
        amount_local_currency DECIMAL(38,2)
            COMMENT 'amount in local currency',
        opening_balance DECIMAL(38,2)
            COMMENT 'opening balance',

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

# COMMAND ----------

