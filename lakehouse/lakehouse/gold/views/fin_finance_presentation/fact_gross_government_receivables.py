# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_finance_presentation.fact_gross_government_receivables
    AS
    SELECT
        ggv.fact_gross_government_receivables_skey,
        ggv.company_code,
        ggv.fiscal_year,
        ggv.fiscal_year_period,
        ggv.posting_period,
        ggv.currency_key,
        ggv.global_currency,        
        ggv.ytd_amounts_in_company_code_currency,
        ggv.ytd_offsets_in_company_code_currency,
        ggv.ytd_payments_in_company_code_currency,
        ggv.ytd_amounts_in_global_currency,
        ggv.ytd_offsets_in_global_currency,
        ggv.ytd_payments_in_global_currency,
        ggv.year_month
    FROM
        {env_vars.gold_catalog}.fin_finance.fact_gross_government_receivables AS ggv;
''')