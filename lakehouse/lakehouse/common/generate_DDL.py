# Databricks notebook source
# Notebook with helper methods for generating DDL statements

# COMMAND ----------

# MAGIC %run ./properties

# COMMAND ----------

# generate Silver table

# COMMAND ----------

dbutils.widgets.text("bronze_name", '', 'bronze_name')
dbutils.widgets.text("schemaversion", '', 'schemaversion')

bronze_name = dbutils.widgets.get("bronze_name")
schemaversion = dbutils.widgets.get("schemaversion")

# COMMAND ----------

table_data = spark.sql(f'''
    SELECT
        t.vivid_schema || "." 
            || coalesce(t.vivid_user_defined_table_name, t.vivid_derived_table_name)
        AS table_name
        ,coalesce(t.vivid_table_comment, t.source_table_description, '')
        AS table_comment
    FROM
        vivid_meta.vivid_meta.vivid_table AS t
    WHERE
        t.source_table_name = "{bronze_name}"

''')

table_values = table_data.first()

new_tablename = f'{env_vars.silver_catalog}.{table_values["table_name"]}_sv{schemaversion}'

# COMMAND ----------

column_data = spark.sql(f'''

    SELECT
        f.source_field_name
        ,f.source_field_primary_key_flag
        ,f.source_data_type
        ,f.source_field_length
        ,f.source_field_decimal_places
        ,f.source_field_position
        ,f.source_long_description
        ,'---' AS brk1
        ,f.vivid_derived_field_name
        ,f.vivid_user_defined_field_name
        ,f.vivid_field_comment
        ,f.vivid_foreign_key_table_field
        ,f.vivid_field_type
        ,f.vivid_field_length
        ,f.vivid_primary_key_flag
    FROM
        vivid_meta.vivid_meta.vivid_field AS f
    WHERE
        f.source_table_name = "{bronze_name}"
        AND f.vivid_include_in_silver_flag = "true"
    ORDER BY
        f.source_field_position

''')

# COMMAND ----------

column_ddl = (
    column_data
    .selectExpr(
        '''coalesce(vivid_user_defined_field_name, vivid_derived_field_name) || ' ' || vivid_field_type
            || ' COMMENT "' || coalesce(vivid_field_comment, source_long_description) 
            || '"' AS col_expr
        '''
    )
)

# COMMAND ----------

sql_txt = f'CREATE TABLE IF NOT EXISTS {new_tablename} ('

sql_txt += ','.join([c[0] for c in column_ddl.collect()])

sql_txt += ' ) COMMENT "' + table_values["table_comment"] + '"'

# COMMAND ----------

sql_txt

# COMMAND ----------

# TODO: add the bronze table name and column names as Tags to each column
# Create a list and loop through it calling the alter table statement