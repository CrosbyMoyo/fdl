# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/hash_utils

# COMMAND ----------

spark.sql(
    f'''
        CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_finance_presentation.fact_finance_transaction_summary AS     
        SELECT 
            {hash_ddl(['fftd.vcode_skey','fftd.gl_account_skey', 'fftd.date_key','fftd.datasource_skey', 'fftd.material_skey', 'fftd.local_currency_skey', 'fftd.group_currency_skey', 'fftd.profit_center_skey', 'fftd.cost_center_skey', 'fftd.company_code_skey', 'fftd.country_key', 'fftd.actual_plan_code'])} AS finance_transaction_details_skey,
            fftd.vcode_skey,
            fftd.gl_account_skey,
            CAST(left(fftd.date_key, 8) || '01' AS DATE) AS date_key,
            fftd.datasource_skey,
            fftd.material_skey,
            fftd.local_currency_skey,
            fftd.group_currency_skey,
            fftd.profit_center_skey,
            fftd.cost_center_skey,
            fftd.company_code_skey,
            fftd.country_key,
            fftd.actual_plan_code,
            fftd.ifrs_flag,
            fftd.document_type,
            SUM(fftd.amount_local_currency)                  AS amount_local_currency,
            SUM(fftd.amount_group_currency)                  AS amount_group_currency,
            SUM(fftd.amount_group_currency_plan_rate)        AS amount_group_currency_plan_rate,
            SUM(fftd.amount_group_currency_month_end)        AS amount_group_currency_month_end,
            SUM(fftd.amount_group_currency_plus_1_year_rate) AS amount_group_currency_plus_1_year_rate,
            SUM(fftd.volume_kg)                       AS volume_kg,
            SUM(fftd.volume_litres_l20)               AS volume_litres_l20,
            SUM(fftd.volume_issued_litres_l20)        AS volume_issued_litres_l20,
            SUM(fftd.vcode_amount_group)              AS vcode_amount_group,
            SUM(fftd.vcode_amount_local)              AS vcode_amount_local,
            MAX(fftd.fx_rate_avg_monthly)             AS fx_rate_avg_monthly,
            MAX(fftd.fx_rate_month_end)               AS fx_rate_month_end,
            MAX(fftd.fx_rate_planning)                AS fx_rate_planning
        FROM
            {env_vars.gold_catalog}.fin_finance.fact_finance_transaction_details AS fftd
        GROUP BY 
            fftd.actual_plan_code,
            fftd.vcode_skey,
            fftd.gl_account_skey,
            fftd.date_key,
            fftd.datasource_skey,
            fftd.material_skey,
            fftd.local_currency_skey,
            fftd.group_currency_skey,
            fftd.profit_center_skey,
            fftd.cost_center_skey,
            fftd.company_code_skey,
            fftd.country_key,
            fftd.ifrs_flag,
            fftd.document_type
''')

# COMMAND ----------


spark.sql(f'''
    GRANT ALL PRIVILEGES
    ON VIEW {env_vars.gold_catalog}.fin_finance_presentation.fact_finance_transaction_summary
    TO `data-engineers`;
''')