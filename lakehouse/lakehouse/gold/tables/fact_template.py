# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.gold_catalog}.{schema}.{tablename}
    (   
        -- Surrogate Key
        fact_{tablename}_skey BIGINT
            CONSTRAINT fact_{tablename}_skey PRIMARY KEY
            COMMENT 'Surrogate Key to the fact table',

        -- Business Keys
        date_key DATE
            CONSTRAINT fact_{tablename}__dim_date_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.ca_cross_application_components.dim_date(date_key)
            COMMENT 'FK to dim_date',

        -- Measures
        measure_a INTEGER
            COMMENT '{Measure Description}',

        -- Metadata
        __etl_fprint BIGINT
            COMMENT 'The xxhash64 of the columns that make up this row: FKs and payload combined',
        __etl_load_timestamp TIMESTAMP
            COMMENT 'datetime that the row was added to the table',
        __etl_is_active BOOLEAN
            COMMENT 'Flag indicating the active record. Note: there should only be 1 _is_active for any _etl_keys_fprint',
        __etl_is_deleted BOOLEAN
            COMMENT 'Flag showing if the record has been deleted from the source system'
    )
    COMMENT 
        '{Table description}'
    CLUSTER BY
        AUTO;
''')