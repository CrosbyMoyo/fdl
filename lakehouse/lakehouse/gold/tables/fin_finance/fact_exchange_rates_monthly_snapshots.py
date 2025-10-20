# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.gold_catalog}.fin_finance.fact_exchange_rates_monthly_snapshots (
        -- PK
        fact_exchange_rates_monthly_snapshots_skey BIGINT
            CONSTRAINT fact_exchange_rates_monthly_snapshots__pk PRIMARY KEY
            COMMENT 'Surrogate Key for the fact table',

        -- FKs
        from_country_key STRING
            CONSTRAINT fact_exchange_rates_monthly_snapshots__dim_country__fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.ca_cross_application_components.dim_country(country_key)
            COMMENT 'Country key',

        snapshot_month_key DATE
            CONSTRAINT fact_exchange_rates_monthly_snapshots__snapshot_month__dim_date__fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.ca_cross_application_components.dim_date(date_key)
            COMMENT 'Snapshot month key',

        opening_month_key DATE
            CONSTRAINT fact_exchange_rates_monthly_snapshots__opening_month__dim_date__fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.ca_cross_application_components.dim_date(date_key)
            COMMENT 'Opening month key',

        month_key DATE
            CONSTRAINT fact_exchange_rates_monthly_snapshots__month__dim_date__fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.ca_cross_application_components.dim_date(date_key)
            COMMENT 'Current month key',

        -- Payload
        opening_month_flag BOOLEAN
            COMMENT 'Flag to indicate whether this is the opening month',
        month_on_month_index decimal(26, 21)
            COMMENT 'Month on month index',
        month_on_month_index_weighted decimal(38,6)
            COMMENT 'Weighted month on month index',
        opening_month_on_month_index_weighted decimal(38, 6)
            COMMENT 'Opening weighted month on month index',
        month_on_month_movement decimal(38, 6)
            COMMENT 'Difference between opening and current month index',

        -- Metadata
        __etl_fprint BIGINT
            COMMENT ' the xxhash64 of all the columns that make up the row payload (i.e. all the non-key, and non-metadata columns). Note: all columns must be NOT NULL for the hash to calculate properly.',
        __etl_load_timestamp DATE
            COMMENT 'date (as DATE datatype) that row is effective from. For an updated record this is the previous _effective_to date + 1 day.',
        __etl_is_active BOOLEAN
            COMMENT 'boolean flag indicating the active record. Note: there should only be 1 _is_active for any _etl_keys_fprint',
        __etl_is_deleted BOOLEAN
            COMMENT 'boolean showing if the record has been deleted from the source system'
    )
    CLUSTER BY 
        AUTO;
''')