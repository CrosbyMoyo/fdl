# Databricks notebook source
# MAGIC %md
# MAGIC ## general_schema_ddl
# MAGIC
# MAGIC Idempotent script to set up the schemas for the general catalog
# MAGIC

# COMMAND ----------

# MAGIC %run ../../common/properties

# COMMAND ----------

# Fivetran
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.general_catalog}.fivetran_repo')
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.general_catalog}.fivetran_internal') # TODO: delete this once burst data in brz schema

# Artefacts
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.general_catalog}.artefacts')

# FinOps
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.general_catalog}.finops')