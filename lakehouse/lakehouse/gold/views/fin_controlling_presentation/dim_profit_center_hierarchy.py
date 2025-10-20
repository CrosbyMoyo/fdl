# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_controlling_presentation.dim_profit_center_hierarchy
    AS
    SELECT
        dpch.hierarchy_id
        ,dpch.profit_center_hierarchy_node
        ,dpch.hierarchy_level
        ,dpch.is_leaf_node
        ,dpch.controlling_area
        ,dpch.hierarchy_name
        ,dpch.level_1_node
        ,dpch.level_1_node_text
        ,dpch.level_1_node_sort_order
        ,dpch.level_2_node
        ,dpch.level_2_node_text
        ,dpch.level_2_node_sort_order
        ,dpch.level_3_node
        ,dpch.level_3_node_text
        ,dpch.level_3_node_sort_order
        ,dpch.level_4_node
        ,dpch.level_4_node_text
        ,dpch.level_4_node_sort_order
        ,dpch.level_5_node
        ,dpch.level_5_node_text
        ,dpch.level_5_node_sort_order
        ,dpch.level_6_node
        ,dpch.level_6_node_text
        ,dpch.level_6_node_sort_order
    FROM
        {env_vars.gold_catalog}.fin_controlling.dim_profit_center_hierarchy AS dpch;

''')