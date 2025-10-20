# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.gold_catalog}.fin_general_ledger.fact_manual_inputs
    (
      -- Keys
      input_id STRING 
      COMMENT 'Input ID which is a concatenation between user_email and timestamp',

      -- Payload
      user_email STRING COMMENT 'Email of the user',
      user_name STRING COMMENT 'Name of the user',
      country STRING COMMENT 'Country of the user',
      group_code STRING COMMENT 'Group code associated with the input',
      page STRING COMMENT 'Page number',
      input_type STRING COMMENT 'Feedback indicator',
      period STRING COMMENT 'Period of the input',
      value STRING COMMENT 'Value of the input',
      active_flag BOOLEAN COMMENT 'Active flag',
      timestamp STRING COMMENT 'Timestamp of the input',

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
         COMMENT "Flag indicating the active record. Note: there should only be 1 _is_active for any _etl_keys_fprint",
      __etl_is_deleted BOOLEAN
         COMMENT "Showing if the record has been deleted from the source system"
    )
    CLUSTER BY
        AUTO;
''')