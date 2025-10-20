# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.dim_profit_loss_subcategory
    AS
    SELECT
        -- TO DO: Rename the field names
        pls.vcode,
        pls.sub_category AS Subcat,
        pls.parent AS Parent,
        pls.parent_category AS ParentCat,
        pls.master_parent AS MasterParent,
        pls.master_parent_category AS MasterParentCat
    FROM
        {env_vars.gold_catalog}.fin_general_ledger.dim_profit_loss_subcategory AS pls
''')

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'''
        GRANT ALL PRIVILEGES
        ON VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.dim_profit_loss_subcategory
        TO `data-engineers`;
    ''')