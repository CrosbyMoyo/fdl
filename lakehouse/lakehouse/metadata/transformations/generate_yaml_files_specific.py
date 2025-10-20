# Databricks notebook source
# Run this file to get specific YAML files using the databricks widgets.

# COMMAND ----------

# MAGIC %run ./yaml_metadata

# COMMAND ----------

def get_tables_dict_and_missing_tables(table_names: list):
    vivid_tables_and_source_systems = [row for row in vivid_table.select(
        'source_system',
        'source_table_name',
        'vivid_schema'
    ).collect() if row['source_table_name'] in table_names]

    # log the tables that does not exist in the vivid_table and they are inputted to the widget
    existing_tables = [row['source_table_name'] for row in vivid_tables_and_source_systems]
    missing_tables = [
        table for table in table_names 
        if table not in existing_tables
        ]

    vivid_tables_dict = {
        row['source_table_name']: {
            'source_system': row['source_system'], 
            'vivid_schema': row['vivid_schema'] if row['vivid_schema'] is not None else "common"
            } for row in vivid_tables_and_source_systems
        }

    return vivid_tables_dict, missing_tables


# COMMAND ----------

dbutils.widgets.text("table_names", "table1,table2", "Enter table names (comma-separated)")
table_names_input = dbutils.widgets.get("table_names")

# COMMAND ----------

list_of_tables = [table.strip() for table in table_names_input.split(',')]

vivid_tables_dict, missing_tables = get_tables_dict_and_missing_tables(list_of_tables)

# COMMAND ----------

generate_yaml = GenerateYaml()
path = generate_yaml.generate(vivid_tables_dict)

if missing_tables:
    dbutils.notebook.exit(f"The following tables are missing from the vivid_table and no YAML files created: {missing_tables}, the following tables YAML files are created: {', '.join(vivid_tables_dict.keys())}")
else:
    dbutils.notebook.exit(f"The following tables YAML files are created: {', '.join(vivid_tables_dict.keys())} in {path}")