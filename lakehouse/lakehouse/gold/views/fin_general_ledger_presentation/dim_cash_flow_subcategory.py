# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.dim_cash_flow_subcategory
    AS
    SELECT
        cfc.cf_subcategory_skey,
        cfc.category_rank,
        cfc.node,
        cfc.category_id,
        cfc.sub_category_description,
        cfc.category_description
    FROM
        {env_vars.gold_catalog}.fin_general_ledger.dim_cash_flow_subcategory AS cfc
''')