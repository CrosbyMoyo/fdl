# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_finance_presentation.fact_capex_ceilings
    AS
    SELECT
        f.fact_capex_ceilings_skey,
        f.budget_year,
        f.company_code_skey,
        f.line_of_business_skey,
        f.country_skey,
        f.subtype_name_skey,
        f.currency_skey,
        f.ceiling
    FROM
        {env_vars.gold_catalog}.fin_finance.fact_capex_ceilings AS f;
''')

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'''
        GRANT ALL PRIVILEGES
        ON VIEW {env_vars.gold_catalog}.fin_finance_presentation.fact_capex_ceilings
        TO `data-engineers`;
    ''')