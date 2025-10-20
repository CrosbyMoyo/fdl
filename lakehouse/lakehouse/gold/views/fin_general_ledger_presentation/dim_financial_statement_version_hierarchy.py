# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.dim_financial_statement_version_hierarchy
    AS
    SELECT
        fsv.financial_statement_version_skey
        ,fsv.hierarchy_id
        ,fsv.hierarchy_name
        ,fsv.gl_account
        ,fsv.chart_of_accounts
        ,fsv.hierarchy_level
        ,fsv.is_leaf_node
        ,fsv.level_1_node
        ,fsv.level_1_node_text
        ,fsv.level_2_node
        ,fsv.level_2_node_text
        ,fsv.level_3_node
        ,fsv.level_3_node_text
        ,fsv.level_4_node
        ,fsv.level_4_node_text
        ,fsv.level_5_node
        ,fsv.level_5_node_text
        ,fsv.level_6_node
        ,fsv.level_6_node_text
        ,fsv.level_7_node
        ,fsv.level_7_node_text
        ,fsv.level_8_node
        ,fsv.level_8_node_text
        ,fsv.level_9_node
        ,fsv.level_9_node_text
    FROM
        {env_vars.gold_catalog}.fin_general_ledger.dim_financial_statement_version_hierarchy AS fsv;
''')

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'''
        GRANT ALL PRIVILEGES
        ON VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.dim_financial_statement_version_hierarchy
        TO `data-engineers`;
    ''')