# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.gold_catalog}.fin_general_ledger.fact_commentary
    (
      -- Keys
      comment_id STRING 
      COMMENT 'Comment ID which is a concatenation between comment, feedback and commenter_email',

      -- Payload
      commenter_email STRING COMMENT 'Email of the commenter',
      timestamp STRING COMMENT 'Timestamp of the comment',
      comment STRING COMMENT 'The comment text',
      commenter_name STRING COMMENT 'Name of the commenter',
      region STRING COMMENT 'Region of the commenter',
      country STRING COMMENT 'Country of the commenter',
      feedback_ind BOOLEAN COMMENT 'Feedback indicator',
      group_code STRING COMMENT 'Group code associated with the comment',
      month STRING COMMENT 'Month of the comment',
      page STRING COMMENT 'Page number',
      report STRING COMMENT 'Report name',
      soft_delete BOOLEAN COMMENT 'Soft delete flag',
      subject STRING COMMENT 'Subject of the comment',
      year STRING COMMENT 'Year of the comment',

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
         COMMENT "showing if the record has been deleted from the source system"
    )
    CLUSTER BY
        AUTO;
''')