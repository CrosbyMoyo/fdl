# Databricks notebook source
# Run this file to regenarate the updated column transformations for YAML files using the databricks widgets.

# COMMAND ----------

# MAGIC %run ../../common/properties

# COMMAND ----------

# MAGIC %run ./yaml_metadata

# COMMAND ----------

dbutils.widgets.text("table_name", "", "Enter a bronze table name")
table_name_input = dbutils.widgets.get("table_name")
logger.log.info(f'Widget: table_name = "{table_name_input}"')

# COMMAND ----------

dbutils.widgets.text("schema_name", "", "Enter the schema name (directory for YAML file)")
schema_name_input = dbutils.widgets.get("schema_name")
logger.log.info(f'Widget: schema_name = "{schema_name_input}"')

# COMMAND ----------

table_name = table_name_input.strip()
schema_name = schema_name_input.strip()

# COMMAND ----------

generate_yaml = GenerateYaml()
generate_yaml.regenerate(schema_name, table_name)