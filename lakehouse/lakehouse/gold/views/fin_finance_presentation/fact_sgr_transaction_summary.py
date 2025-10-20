# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_finance_presentation.fact_sgr_transaction_summary
    AS
        SELECT
            -- Skeys
            ftd.fact_sgr_transaction_summary_skey,
            ftd.datasource,
            ftd.date_key,
            ftd.profit_center_skey,
            ftd.company_code_skey,
            ftd.gl_account_skey,
            ftd.financial_statement_item_skey,
            ftd.consolidation_unit_skey,
            ftd.consolidation_reporting_item_skey,
            ftd.consolidation_segment_skey,
            ftd.posting_level_skey,
            ftd.local_currency_skey,
            ftd.group_currency_skey,
            -- Payload
            ftd.period_mode,
            ftd.consolidation_version,
            ftd.consolidation_document_type,
            ftd.fiscal_year,
            ftd.fiscal_period,
            ftd.amount_local_currency,
            ftd.amount_group_currency
        FROM
            {env_vars.gold_catalog}.fin_finance.fact_sgr_transaction_summary AS ftd
''')

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'''
        GRANT ALL PRIVILEGES
        ON VIEW {env_vars.gold_catalog}.fin_finance_presentation.fact_sgr_transaction_summary
        TO `data-engineers`;
    ''')