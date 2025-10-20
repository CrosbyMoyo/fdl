# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.dim_profit_loss_category
    AS
    SELECT
        -- TO DO: Rename the field names
        plc.rank AS Rnk,
        plc.parent_id AS ParentID,
        plc.pl_category AS PLCat
    FROM
        {env_vars.gold_catalog}.fin_general_ledger.dim_profit_loss_category AS plc
''')

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'''
        GRANT ALL PRIVILEGES
        ON VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.dim_profit_loss_category
        TO `data-engineers`;
    ''')