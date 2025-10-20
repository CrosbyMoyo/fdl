# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
   CREATE OR REPLACE TABLE {env_vars.gold_catalog}.ca_cross_application_components.dim_datasource
   (
        datasource_skey BIGINT
         COMMENT 'Surrogate key for the datasource dimension'
         CONSTRAINT dim_datasource_pk PRIMARY KEY,
        datasource_name STRING
         COMMENT 'Unique identifier for the datasource',
        datasource_description STRING
         COMMENT 'Description of the datasource',
         
        -- Metadata columns
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
        COMMENT "showing if the record has been deleted from the source system"
    )
    CLUSTER BY
        AUTO;
''')