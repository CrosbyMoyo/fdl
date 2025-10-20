# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_controlling_presentation.dim_cost_center
    AS
    SELECT
        dcc.cost_center_skey
        ,dcc.controlling_area
        ,dcc.cost_center
        ,dcc.valid_to
        ,dcc.budget_holder
        ,dcc.cost_center_description
        ,dcc.valid_from
        ,dcc.company_code
        ,dcc.cost_center_category
        ,dcc.cost_center_category_description
        ,dcc.profit_center
        ,dcc.department
    FROM
        {env_vars.gold_catalog}.fin_controlling.dim_cost_center AS dcc;

''')