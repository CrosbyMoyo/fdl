# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.dim_gl_account
    AS
    SELECT
        gla.gl_account_skey,
        gla.chart_of_accounts,
        gla.gl_account,
        gla.gl_account_description,
        gla.balance_sheet_account_flag,
        gla.group_account_number,
        gla.functional_area,
        gla.gl_account_type,
        gla.gl_account_subtype,
        gla.last_changed_timestamp
    FROM
        {env_vars.gold_catalog}.fin_general_ledger.dim_gl_account AS gla;

''')

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'''
        GRANT ALL PRIVILEGES
        ON VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.dim_gl_account
        TO `data-engineers`;
    ''')