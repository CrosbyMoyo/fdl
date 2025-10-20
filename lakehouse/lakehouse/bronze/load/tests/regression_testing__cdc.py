# Databricks notebook source
dbutils.widgets.text('env', '')
dbutils.widgets.text('source_system', '')

dbutils.widgets.text('bronze_catalog', '')
dbutils.widgets.text('bronze_schema', '')
dbutils.widgets.text('bronze_table', '')

notebook_params = dbutils.widgets.getAll()

# COMMAND ----------

primary_key_columns = spark.sql(f"""
    select distinct 
        source_field_name
    from
        vivid_meta.vivid_meta.vivid_field
    where
        source_table_name = '{notebook_params["bronze_table"]}'
        and source_system = '{notebook_params["source_system"]}'
        and source_field_primary_key_flag
"""
).collect()

primary_key = [key.source_field_name for key in primary_key_columns]

# COMMAND ----------

bronze_table = f"{notebook_params['bronze_catalog']}.{notebook_params['bronze_schema']}.{notebook_params['bronze_table']}"

# Hop 1 
bronze_cdc_table = f"{notebook_params['bronze_catalog']}.{notebook_params['bronze_schema']}.{notebook_params['bronze_table']}__cdc"

# COMMAND ----------

source_distinct_keys = spark.sql(f"""
    SELECT DISTINCT 
        {','.join([f"b.{key}" for key in primary_key])}
    FROM 
        {bronze_table} AS b
    GROUP BY 
        {','.join([f"b.{key}" for key in primary_key])}
""")

source_distinct_keys.createOrReplaceTempView('source_distinct_keys')

# COMMAND ----------

target_distinct_keys = spark.sql(f"""
    SELECT DISTINCT 
        {','.join([f"b.{key}" for key in primary_key])}
    FROM 
        {bronze_cdc_table} AS b
    GROUP BY 
        {','.join([f"b.{key}" for key in primary_key])}
""")

target_distinct_keys.createOrReplaceTempView('target_distinct_keys')

# COMMAND ----------

anti_join_result = spark.sql(f"""
    SELECT 
        source.*
    FROM 
        source_distinct_keys AS source
    LEFT JOIN 
        target_distinct_keys AS target
    ON 
        {' AND '.join([f"source.{key} = target.{key}" for key in primary_key])}
""")

display(anti_join_result)