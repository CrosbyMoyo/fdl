# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_finance_presentation.dim_capex_subtype
    AS
    SELECT
        st.subtype_name_skey
        ,st.model_subtype_id
        ,st.subtype_name
    FROM
        {env_vars.gold_catalog}.fin_finance.dim_capex_subtype AS st;

''')