# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.dim_consolidation_unit_hierarchy
    AS
    SELECT
        dvc.hierarchy_id
        ,dvc.consolidation_unit_hierarchy_node
        ,dvc.consolidation_unit_description
        ,dvc.hierarchy_level
        ,dvc.is_leaf_node
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
    FROM
        {env_vars.gold_catalog}.fin_general_ledger.dim_consolidation_unit_hierarchy AS dvc;
''')

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'''
        GRANT ALL PRIVILEGES
        ON VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.dim_consolidation_unit_hierarchy
        TO `data-engineers`;
    ''')