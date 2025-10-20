# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_finance_presentation.fact_oil_prices_agg
    AS
    SELECT
        f.fact_oil_prices_agg_skey,
        f.year_month,
        f.symbol,
        f.avg_price
    FROM
        {env_vars.gold_catalog}.fin_finance.fact_oil_prices_agg AS f;
''')

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'''
        GRANT ALL PRIVILEGES
        ON VIEW {env_vars.gold_catalog}.fin_finance_presentation.fact_oil_prices_agg
        TO `data-engineers`;
    ''')