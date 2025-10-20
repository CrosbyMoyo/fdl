# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.dim_working_capital_category
    AS
    SELECT
        wc_cat.working_capital_category_skey,
        wc_cat.rank,
        wc_cat.parent_id,
        wc_cat.wc_category_node,
        wc_cat.wc_category
    FROM
        {env_vars.gold_catalog}.fin_general_ledger.dim_working_capital_category AS wc_cat
''')