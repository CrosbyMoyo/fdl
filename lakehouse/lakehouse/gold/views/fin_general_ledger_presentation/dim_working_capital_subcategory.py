# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.dim_working_capital_subcategory
    AS
    SELECT
        wcs.working_capital_subcategory_skey,
        wcs.category_rank,
        wcs.node,
        wcs.category_id,
        wcs.sub_category_description,
        wcs.category_description
    FROM
        {env_vars.gold_catalog}.fin_general_ledger.dim_working_capital_subcategory AS wcs
''')