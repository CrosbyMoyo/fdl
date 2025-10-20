# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_finance_presentation.fact_exchange_rates_monthly_snapshots AS 
    SELECT 
        -- Keys
         f.fact_exchange_rates_monthly_snapshots_skey 
        ,f.from_country_key
        ,f.snapshot_month_key
        ,f.opening_month_key
        ,f.month_key
        -- Payload
        ,f.opening_month_flag
        ,f.month_on_month_index
        ,f.month_on_month_index_weighted
        -- TODO: Change this and let the front end devs know 
        ,f.opening_month_on_month_index_weighted AS opening_month_on_month_weighted
        ,f.month_on_month_movement
    FROM 
        {env_vars.gold_catalog}.fin_finance.fact_exchange_rates_monthly_snapshots AS f 

''')