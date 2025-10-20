# Databricks notebook source
# MAGIC %md
# MAGIC ## Metadata: Generate Silver Metadata YAML File
# MAGIC This notebook generates the silver metadata YAML files for specific tables based off the configurations set in the `vivid_meta` database and writes it to the current workspace directory.
# MAGIC
# MAGIC ### Before you start:
# MAGIC - Make sure you're working in a branch and not directly on the workspace, it will fail otherwise!
# MAGIC
# MAGIC ### Parameters:
# MAGIC - Table_Name: The name of the table in vivid_meta to generate the YAML for
# MAGIC - Source_System: The name of the source system in vivid_meta of the table

# COMMAND ----------

# MAGIC %run ../metadata/transformations/yaml_metadata

# COMMAND ----------

dbutils.widgets.text("Table_Name", "", "Enter Table Name")
dbutils.widgets.text("Source_System", "", "Enter Source System Name")

# COMMAND ----------

table_name = dbutils.widgets.get("Table_Name")
source_system = dbutils.widgets.get("Source_System")

# COMMAND ----------

spark.sql(f'''
    SELECT
        t.*
    FROM
        vivid_meta.vivid_meta.vivid_table AS t
    WHERE
        t.source_table_name = '{table_name}'
        and t.source_system = '{source_system}'
''').display()

# COMMAND ----------

generate_yaml = GenerateYaml([table_name])
print(f"YAML file created at: {generate_yaml.path}")