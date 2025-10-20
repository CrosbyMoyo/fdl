# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_finance_presentation.fact_gross_government_receivables_ds
    AS
    SELECT
        f.company_code_skey,
        f.payments,
        f.amount_gbr_usd,
        f.amount_local_currency,
        f.opening_balance

    FROM
        {env_vars.gold_catalog}.fin_finance.fact_gross_government_receivables_ds AS f;
''')

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'''
        GRANT ALL PRIVILEGES
        ON VIEW {env_vars.gold_catalog}.fin_finance_presentation.fact_gross_government_receivables_ds
        TO `data-engineers`;
    ''')