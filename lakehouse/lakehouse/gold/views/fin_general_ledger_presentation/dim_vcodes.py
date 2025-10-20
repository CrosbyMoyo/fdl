# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.dim_vcodes
    AS
    SELECT
        dvc.vcode_skey
        ,dvc.vcode
        ,dvc.description
        ,dvc.c1_relevant_flag
        ,dvc.c2_relevant_flag
        ,dvc.c3_relevant_flag
        ,dvc.c4_relevant_flag
        ,dvc.net_income_relevant_flag
        ,dvc.local_ebitda_relevant_flag
        ,dvc.local_opex
        ,dvc.opex_description
        ,dvc.opex_type
        ,dvc.opex_type_description
        ,dvc.direct_contribution_relevant_flag
        ,dvc.indirect_contribution_relevant_flag
        ,dvc.vcode_sort_order
    FROM
        {env_vars.gold_catalog}.fin_general_ledger.dim_vcodes AS dvc;

''')

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'''
        GRANT ALL PRIVILEGES
        ON VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.dim_vcodes
        TO `data-engineers`;
    ''')