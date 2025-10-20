# Databricks notebook source
# MAGIC %run ./properties

# COMMAND ----------

# TODO: turn this into a def

# COMMAND ----------

schema_name = 'fin_general_ledger'
table_name = 'universal_journal_line_items'

# COMMAND ----------

dbutils.widgets.text(
    name='catalog_name',
    defaultValue='',
    label='1 - Catalog Name'
)
# TODO: make this a dropdown

dbutils.widgets.text(
    name='schema_name',
    defaultValue='',
    label='2 - Schema Name'
)

dbutils.widgets.text(
    name='table_name',
    defaultValue='',
    label='3 - Table Name'
)

# COMMAND ----------

dest_table_name = f'{env_vars.silver_catalog}.{schema_name}.{table_name}'

# COMMAND ----------

table_metadata = spark.sql(f'''
    SELECT
        t.source_system
        ,t.source_table_name
        ,t.source_table_description
    FROM
        vivid_meta.vivid_meta.vivid_table AS t
    WHERE
        '{table_name}' IN (
            t.vivid_derived_table_name,
            t.vivid_user_defined_table_name
        );
''')

# COMMAND ----------

table_tags = table_metadata.collect()[0].asDict()

table_tag_dict = [
    f'"{t[0]}" = "{t[1]}"'
    for t in table_tags.items()
]

table_tag_str = ', '.join(t for t in table_tag_dict)

# COMMAND ----------

table_result = spark.sql(f'''
    ALTER TABLE {dest_table_name}
    SET TAGS (
        {table_tag_str}
    )
''')

# COMMAND ----------

column_metadata = spark.sql(f'''
    SELECT
        f.source_table_name
        ,f.source_field_name
        ,f.source_field_primary_key_flag
        ,coalesce(
            f.vivid_user_defined_field_name,
            f.vivid_derived_field_name
        ) AS field_name
    FROM
        vivid_meta.vivid_meta.vivid_field AS f
    WHERE
        f.source_table_name = '{table_tags["source_table_name"]}'
        AND f.vivid_include_in_silver_flag = true;
''')

# COMMAND ----------

for col in column_metadata.collect():

    column_tag_dict = [
        f'"{t[0]}" = "{t[1]}"'
        for t in col.asDict().items()
        if t[0] != 'field_name'
    ]

    column_tag_str = ', '.join(t for t in column_tag_dict)

    spark.sql(f'''
        ALTER TABLE {dest_table_name}
        ALTER COLUMN {col['field_name']}
        SET TAGS (
            {column_tag_str}
        )
    ''')

    print(f'Updated column: {col["field_name"]}')