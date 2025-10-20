# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
   CREATE OR REPLACE TABLE {env_vars.gold_catalog}.ca_cross_application_components.dim_version
   (
      version_skey BIGINT
         CONSTRAINT dim_version_pk PRIMARY KEY
         COMMENT 'Surrogate key for the version dimension',
      version_id STRING
         COMMENT 'Unique identifier for the version',
      version_description STRING
         COMMENT 'Description of the version'
   )
   CLUSTER BY
      AUTO;
''')