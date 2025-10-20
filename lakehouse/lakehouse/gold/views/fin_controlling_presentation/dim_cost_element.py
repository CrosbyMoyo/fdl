# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_controlling_presentation.dim_cost_element
    AS
    SELECT
        dcc.cost_element_skey
        ,dcc.chart_of_accounts
        ,dcc.cost_element
        ,dcc.cost_element_description_long
        ,dcc.primary_cost
        ,dcc.primary_cost_description
        ,dcc.total_opex
        ,dcc.total_opex_description
    FROM
        {env_vars.gold_catalog}.fin_controlling.dim_cost_element AS dcc;
''')