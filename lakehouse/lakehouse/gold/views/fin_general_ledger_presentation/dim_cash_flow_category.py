# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.dim_cash_flow_category
    AS
    SELECT
        cfc.cf_category_skey,
        cfc.rank,
        cfc.parent_id,
        cfc.cf_category_node,
        cfc.cf_category
    FROM
        {env_vars.gold_catalog}.fin_general_ledger.dim_cash_flow_category AS cfc
''')