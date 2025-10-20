# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_controlling_presentation.fact_special_items
    AS
    SELECT
        f.year_month,
        f.special_item_code,
        f.amount_mtd,
        f.amount_ytd
    FROM
        {env_vars.gold_catalog}.fin_controlling.fact_special_items AS f;
''')

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'''
        GRANT ALL PRIVILEGES
        ON VIEW {env_vars.gold_catalog}.fin_controlling_presentation.fact_special_items
        TO `data-engineers`;
    ''')