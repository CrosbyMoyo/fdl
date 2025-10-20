# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_finance_presentation.fact_finance_exposure
    AS
    SELECT
        f.fact_finance_exposure_skey,
        f.snapshot_date_key,
        f.company_code_skey,
        f.datasource_skey,
        f.goods_received_not_invoiced_in_group_currency,
        f.accounts_payable_in_group_currency,
        f.accounts_receivable_group_currency,
        f.general_ledger_borrowings_group_currency,
        f.imports,
        f.exports,
        f.reporting_currency,
        f.cash_balance,
        f.goods_received_not_invoiced_in_local_currency,
        f.accounts_receivable_in_local_currency,
        f.imports_in_local_currency,
        f.exports_in_local_currency,
        f.accounts_payable_in_local_currency,
        f.overdraft_balance,
        f.document_currency,
        f.dividends_payable,
        f.product_price_not_delivered
    FROM
        {env_vars.gold_catalog}.fin_finance.fact_finance_exposure AS f;
''')

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'''
        GRANT ALL PRIVILEGES
        ON VIEW {env_vars.gold_catalog}.fin_finance_presentation.fact_finance_exposure
        TO `data-engineers`;
    ''')