# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.dim_leveraged_free_cash_flow_category
    AS
    SELECT
        lfcf_cat.leveraged_free_cash_flow_category_skey,
        lfcf_cat.rank,
        lfcf_cat.parent_id,
        lfcf_cat.lfcf_category_node,
        lfcf_cat.lfcf_category
    FROM
        {env_vars.gold_catalog}.fin_general_ledger.dim_leveraged_free_cash_flow_category AS lfcf_cat
''')