# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.{schema}_presentation.{gold_table}
    AS
    SELECT
        g.column_as,
        g.column_b,
        g.column_c
    FROM
        {env_vars.gold_catalog}.{schema}.{gold_table} AS g;
''')
