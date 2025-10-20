# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.dim_vcodes_hierarchy
    AS
    SELECT
        dvc.hierarchy_id
        ,dvc.vcode_hierarchy_node
        ,dvc.vcode_description
        ,dvc.hierarchy_level
        ,dvc.is_leaf_node
        ,dvc.hierarchy_name
        ,dvc.level_1_node
        ,dvc.level_1_node_text
        ,dvc.level_2_node
        ,dvc.level_2_node_text
        ,dvc.level_3_node
        ,dvc.level_3_node_text
        ,dvc.level_4_node
        ,dvc.level_4_node_text
        ,dvc.level_5_node
        ,dvc.level_5_node_text
        ,dvc.level_6_node
        ,dvc.level_6_node_text
        ,dvc.level_7_node
        ,dvc.level_7_node_text
        ,dvc.level_8_node
        ,dvc.level_8_node_text
        ,dvc.level_9_node
        ,dvc.level_9_node_text
        ,dvc.level_10_node
        ,dvc.level_10_node_text
        ,dvc.level_11_node
        ,dvc.level_11_node_text
        ,dvc.level_12_node
        ,dvc.level_12_node_text
        ,dvc.level_13_node
        ,dvc.level_13_node_text
        ,dvc.level_14_node
        ,dvc.level_14_node_text
    FROM
        {env_vars.gold_catalog}.fin_general_ledger.dim_vcodes_hierarchy AS dvc;
''')

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'''
        GRANT ALL PRIVILEGES
        ON VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.dim_vcodes_hierarchy
        TO `data-engineers`;
    ''')