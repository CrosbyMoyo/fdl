# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.dim_trial_balance_assets_category
    AS
    SELECT
        tbcat.trial_balance_assets_category_skey,
        tbcat.rank,
        tbcat.parent_id,
        tbcat.tb_category_node,
        tbcat.tb_category,
        tbcat.report_category
    FROM
        {env_vars.gold_catalog}.fin_general_ledger.dim_trial_balance_assets_category AS tbcat
''')