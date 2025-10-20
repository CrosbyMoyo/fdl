# Databricks notebook source
# Note: Do not run this notebook, to generate the YAML files one of the notebooks can be used.

# COMMAND ----------

!pip install ruamel.yaml

# COMMAND ----------

import yaml, os, io
from datetime import datetime, date
from ruamel.yaml import YAML

# COMMAND ----------

vivid_field = spark.read.table('vivid_meta.vivid_meta.vivid_field')
vivid_table = spark.read.table('vivid_meta.vivid_meta.vivid_table')

# COMMAND ----------

# edit the yaml dumper to change the format of the yaml files
class IndentedDumper(yaml.Dumper):
    def increase_indent(self, flow=False, indentless=False):
        return super().increase_indent(flow, False)  # Force extra indentation for the format of the yaml files

# COMMAND ----------

from pyspark.sql.functions import col, when, concat, lit

yaml_data = (
    vivid_field
    .select(
        'source_table_name',
        'source_field_name',
        'source_field_primary_key_flag',
        'vivid_derived_field_name',
        'vivid_user_defined_field_name',
        'vivid_include_in_silver_flag',
        'vivid_field_type',
        when(
            col('vivid_field_type') == 'DATE',
            concat(
                lit("ca_cross_application_components.extract_sap_date("),
                col("vivid_derived_field_name"),
                lit(")")
            )
        ).when(
            col("vivid_field_type") == 'TIMESTAMP',
            concat(
                lit("ca_cross_application_components.extract_sap_timestamp("),
                col("vivid_derived_field_name"),
                lit(")")
            )
        ).otherwise(None).alias('vivid_complex_cast_function')
    )
    .orderBy('source_field_position')
)

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

class GenerateYaml():
    "Generate the YAML files that are used in bronze to silver transformations."
    def __init__(self, tables: []):
        self.vivid_field = vivid_field
        self.vivid_table = vivid_table
        
        assert 'Users' in dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get(), "This can only be run on a branch or your users folder!" 

        table_dict, missing_tables = get_tables_dict_and_missing_tables(tables)

        if missing_tables:
            raise ValueError(f"Table not found: {missing_tables}")

        self.path = self.generate(table_dict)

    def generate(self, vivid_tables_dict):
        for table_name, values in vivid_tables_dict.items():
            yaml_input = {
                'source': {
                    'catalog': "bronze",
                    'schema': values['source_system'],
                    'table': table_name,
                },
                'destinations': [
                    {
                        'stage': "(cast, rename, cleanse, deduplicate, lookup, etc)",
                        'operation': "(insert, overwrite, merge)",
                        'schema': "schema for stage",
                        'table': "table name for stage",
                        'comment': "short description of the stage",
                        'schemaversion': "schema version"
                    }
                ],
                'column_transformations': [
                    {
                        'name': row['source_field_name'], 
                        # use the vivid_user_defined_field_name if it exists, otherwise use the derived field name
                        'rename_to': row['vivid_user_defined_field_name'] if row['vivid_user_defined_field_name'] is not None else row['vivid_derived_field_name'], 
                        'cast_to': row['vivid_field_type'],
                        'complex_cast_function': row['vivid_complex_cast_function'],
                        'surrogate_null': 0 if row['vivid_field_type'] in ["INT", "DECIMAL(u,z)"] 
                                            else datetime(1900, 1, 1).date() if row['vivid_field_type'] == "DATE" 
                                            else datetime(1900, 1, 1, 0, 0, 0) if row['vivid_field_type'] == "TIMESTAMP" 
                                            else "",
                        # check if the field is a primary key, otherwise mark it as a payload
                        'column_role': "PK" if row['source_field_primary_key_flag'] == True else "PAYLOAD",
                    }
                    # loop through the fields filtering by the table name
                    for row in yaml_data.collect() if row['source_table_name'] == table_name and row['vivid_include_in_silver_flag'] == True
                ]
            }

            file_path = (f"../silver/transformations/{values['vivid_schema']}/metadata/silver.{table_name}.yaml")
            yaml_output = yaml.dump(yaml_input, Dumper=IndentedDumper, default_flow_style=False, sort_keys=False)
            
            with open(file_path, "w") as f:
                f.write(yaml_output)
            
        return file_path
    
    def regenerate(self, schema_name, table_name):
        yaml_file_path = f'../silver/transformations/{schema_name}/metadata/silver.{table_name}.yaml'

        with open(yaml_file_path, "r") as f:
            file_content = f.read()
        # 2. Parse with ruamel.yaml
        yaml = YAML()

        def represent_none(self, data):
            return self.represent_scalar('tag:yaml.org,2002:null', 'null')

        yaml.representer.add_representer(type(None), represent_none)

        data = yaml.load(io.StringIO(file_content))

        # 3. Overwrite `data['column_transformations']` with newly generated list
        data['column_transformations'] = [
            {
                'name': row['source_field_name'], 
                'rename_to': row['vivid_user_defined_field_name']
                            if row['vivid_user_defined_field_name'] is not None 
                            else row['vivid_derived_field_name'], 
                'cast_to': row['vivid_field_type'],
                'complex_cast_function': row['vivid_complex_cast_function'] or None,
                'surrogate_null': (
                    0 
                    if row['vivid_field_type'] in ["INT", "DECIMAL(u,z)"] 
                    else datetime(1900, 1, 1).date()
                    if row['vivid_field_type'] == "DATE"
                    else datetime(1900, 1, 1, 0, 0, 0)
                    if row['vivid_field_type'] == "TIMESTAMP"
                    else ""
                ),
                'column_role': "PK" if row['source_field_primary_key_flag'] else "PAYLOAD",
            }
            for row in yaml_data.collect() if row['source_table_name'] == table_name and row['vivid_include_in_silver_flag'] == True
        ]
        # 4. Dump the updated YAML structure to a string
        output_buffer = io.StringIO()
        yaml.dump(data, output_buffer)
        updated_yaml_str = output_buffer.getvalue()

        # 5. Write it back
        with open(yaml_file_path, "w") as f:
            f.write(updated_yaml_str)