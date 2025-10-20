# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_finance_presentation.fact_finance_transaction_details
    AS
    SELECT
        -- PK
        ftd.fact_finance_transaction_details_skey

        -- FKs
        ,ftd.date_key
        ,ftd.profit_center_skey
        ,ftd.line_of_business_skey
        ,ftd.company_code_skey
        ,ftd.country_key
        ,ftd.vcode_skey
        ,ftd.cost_center_skey
        ,ftd.gl_account_skey
        ,ftd.datasource_skey
        ,ftd.material_skey
        ,ftd.local_currency_skey
        ,ftd.group_currency_skey
        ,ftd.actual_plan_code
        ,ftd.ifrs_flag
 
        -- Measures
        ,ftd.amount_local_currency
        ,ftd.local_currency_code
        ,ftd.amount_group_currency
        ,ftd.amount_group_currency_plan_rate
        ,ftd.amount_group_currency_month_end
        ,ftd.amount_group_currency_plus_1_year_rate
        ,ftd.volume_litres_l20
        ,ftd.volume_kg
        ,ftd.volume_issued_litres_l20
        ,ftd.vcode_amount_local
        ,ftd.vcode_amount_group
    FROM
        {env_vars.gold_catalog}.fin_finance.fact_finance_transaction_details AS ftd;
''')

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'''
        GRANT ALL PRIVILEGES
        ON VIEW {env_vars.gold_catalog}.fin_finance_presentation.fact_finance_transaction_details
        TO `data-engineers`;
    ''')