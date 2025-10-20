# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_controlling_presentation.fact_capex_costs
    AS
    SELECT
        f.internal_order,
        f.date_key,
        f.company_code_skey,
        f.subtype_name_skey,
        f.profit_center_skey,
        f.line_of_business_skey,
        f.currency_skey,
        f.plant_skey,
        f.object_number,
        f.order_type,
        f.order_category,
        f.ledger,
        f.fiscal_year,
        f.budget_local,
        f.allocated_local,
        f.budget_usd,
        f.actuals_usd,
        f.committed_usd,
        f.actuals_local,
        f.committed_local
    FROM
        {env_vars.gold_catalog}.fin_controlling.fact_capex_costs AS f;
''')

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'''
        GRANT ALL PRIVILEGES
        ON VIEW {env_vars.gold_catalog}.fin_controlling_presentation.fact_capex_costs
        TO `data-engineers`;
    ''')