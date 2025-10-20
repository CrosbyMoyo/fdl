# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_controlling_presentation.dim_cost_element_hierarchy
    AS
    SELECT
        dcch.hierarchy_id
        ,dcch.cost_element_hierarchy_node
        ,dcch.hierarchy_level
        ,dcch.is_leaf_node
        ,dcch.controlling_area
        ,dcch.hierarchy_name
        ,dcch.level_1_node
        ,dcch.level_1_node_text
        ,dcch.level_1_node_sort_order
        ,dcch.level_2_node
        ,dcch.level_2_node_text
        ,dcch.level_2_node_sort_order
        ,dcch.level_3_node
        ,dcch.level_3_node_text
        ,dcch.level_3_node_sort_order
        ,dcch.level_4_node
        ,dcch.level_4_node_text
        ,dcch.level_4_node_sort_order
        
    FROM
        {env_vars.gold_catalog}.fin_controlling.dim_cost_element_hierarchy AS dcch;

''')