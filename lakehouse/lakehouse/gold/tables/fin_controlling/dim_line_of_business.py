# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.gold_catalog}.fin_controlling.dim_line_of_business
    (
        -- Keys 
        line_of_business_skey BIGINT
            COMMENT 'line of business skey',
        line_of_business STRING
            COMMENT 'line of business',
        -- Payload
        line_of_business_description STRING
            COMMENT 'line of business description',
        class_of_business STRING
            COMMENT 'Class of business',
        class_of_business_description STRING
            COMMENT 'Class of business description',
        -- Metadata
        __etl_keys_fprint BIGINT
            COMMENT "xxhash64 of the Business Keys that this record is made up of",
        __etl_row_fprint BIGINT
            COMMENT "the xxhash64 of all the columns that make up the row payload",
        __etl_effective_from DATE
            COMMENT "Date that row is effective from",
        __etl_effective_to DATE
            COMMENT "Date that row is effective to, or NULL for active record",
        __etl_is_active BOOLEAN
            COMMENT "flag indicating the active record. Note: there should only be 1 _is_active for any _etl_keys_fprint",
        __etl_is_deleted BOOLEAN
            COMMENT "showing if the record has been deleted from the source system",
        CONSTRAINT `pk_dim_line_of_business` PRIMARY KEY (`line_of_business_skey`)
   )
   CLUSTER BY AUTO;
''')