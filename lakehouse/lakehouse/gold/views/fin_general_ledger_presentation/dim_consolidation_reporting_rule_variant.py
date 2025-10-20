# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.dim_consolidation_reporting_rule_variant
    AS
    SELECT
        cu.consolidation_reporting_rule_variant_skey,
        cu.reporting_rule_variant,
        cu.reporting_item_hierarchy,
        cu.consolidation_chart_of_accounts,
        cu.description
    FROM
        {env_vars.gold_catalog}.fin_general_ledger.dim_consolidation_reporting_rule_variant AS cu;

''')

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'''
        GRANT ALL PRIVILEGES
        ON VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.dim_consolidation_reporting_rule_variant
        TO `data-engineers`;
    ''')