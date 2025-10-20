# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.dim_trial_balance_subcategory
    AS
    SELECT
        tbs.trial_balance_subcategory_skey,
        tbs.category_rank,
        tbs.node,
        tbs.category_id,
        tbs.sub_category_description,
        tbs.category_description
    FROM
        {env_vars.gold_catalog}.fin_general_ledger.dim_trial_balance_subcategory AS tbs
''')