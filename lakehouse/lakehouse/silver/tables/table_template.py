# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.silver_catalog}.{schema}.{tablename} (
      -- Keys
      {key_column_a} STRING
          COMMENT '{Column description}',

      -- Payload
      {payload_column_a} INTEGER
          COMMENT '{Column Description}', 

      -- Metadata
      __etl_keys_fprint BIGINT
        COMMENT 'The xxhash64 of the primary key that this record is made up of',
      __etl_row_fprint BIGINT
        COMMENT 'The xxhash64 of all the columns that make up the row payload',
      __etl_effective_from DATE
        COMMENT 'The date that row is effective from',
      __etl_effective_to DATE
        COMMENT 'The date that row is effective to, or NULL for active record',
      __etl_is_active BOOLEAN
        COMMENT 'Flag indicating the active record. Note: there should only be 1 _is_active for any _etl_keys_fprint',
      __etl_is_deleted BOOLEAN
        COMMENT 'Flag showing if the record has been deleted from the source system'
    )
    COMMENT
      '{Table description}'
    CLUSTER BY AUTO;
''')