# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_finance_presentation.fact_exchange_rates
    AS
    SELECT
        fx.fact_exchange_rates_skey
        ,fx.valid_from_key 
        ,fx.from_currency
        ,fx.to_currency
        ,fx.exchange_rate_type
        ,fx.scaled_exchange_rate
    FROM
        {env_vars.gold_catalog}.fin_finance.fact_exchange_rates AS fx;
''')

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'''
        GRANT ALL PRIVILEGES
        ON VIEW {env_vars.gold_catalog}.fin_finance_presentation.fact_exchange_rates
        TO `data-engineers`;
    ''')