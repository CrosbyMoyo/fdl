# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.dim_consolidation_unit
    AS
    SELECT
        cu.consolidation_unit_skey,
        cu.dimension,
        cu.consolidation_unit,
        cu.consolidation_unit_description,
        cu.consolidation_group,
        cu.country_region,
        cu.company
    FROM
        {env_vars.gold_catalog}.fin_general_ledger.dim_consolidation_unit AS cu;

''')

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'''
        GRANT ALL PRIVILEGES
        ON VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.dim_consolidation_unit
        TO `data-engineers`;
    ''')