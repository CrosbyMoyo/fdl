# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.fact_manual_inputs
    AS
    SELECT
        fmi.input_id,
        fmi.user_email,
        fmi.user_name,
        fmi.country,
        fmi.group_code,
        fmi.page,
        fmi.input_type,
        fmi.period,
        fmi.value,
        fmi.active_flag,
        fmi.timestamp
    FROM
        {env_vars.gold_catalog}.fin_general_ledger.fact_manual_inputs AS fmi;
''')