# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.dim_consolidation_segment
    AS
    SELECT
        cs.consolidation_segment_skey,
        cs.segment,
        cs.description
    FROM
        {env_vars.gold_catalog}.fin_general_ledger.dim_consolidation_segment AS cs;

''')

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'''
        GRANT ALL PRIVILEGES
        ON VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.dim_consolidation_segment
        TO `data-engineers`;
    ''')