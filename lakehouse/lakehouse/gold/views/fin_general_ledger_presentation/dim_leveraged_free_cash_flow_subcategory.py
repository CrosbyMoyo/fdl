# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.dim_leveraged_free_cash_flow_subcategory
    AS
    SELECT
        lcfcs.leveraged_free_cash_flow_subcategory_skey,
        lcfcs.category_rank,
        lcfcs.node,
        lcfcs.category_id,
        lcfcs.sub_category_description,
        lcfcs.category_description
    FROM
        {env_vars.gold_catalog}.fin_general_ledger.dim_leveraged_free_cash_flow_subcategory AS lcfcs
''')