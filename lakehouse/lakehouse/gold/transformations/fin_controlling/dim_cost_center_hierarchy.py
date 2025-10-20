# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')


# COMMAND ----------

dest_tablename = f'{env_vars.gold_catalog}.fin_controlling.dim_cost_center_hierarchy'

# COMMAND ----------

spark.sql(
    f'''
        SELECT 
            p.*
        from 
            {env_vars.silver_catalog}.fin_controlling.cost_center_hierarchy AS p
    '''
).createOrReplaceTempView('hashed')

# COMMAND ----------

merge_result = spark.sql(f'''
    MERGE INTO {dest_tablename} AS tgt
    USING hashed AS src
        ON tgt.hierarchy_id = src.hierarchy_id AND 
        tgt.cost_center_hierarchy_node = src.cost_center_hierarchy_node AND
        tgt.controlling_area = src.controlling_area AND
        tgt.hierarchy_level = src.hierarchy_level 
    WHEN MATCHED THEN
        UPDATE SET 
            tgt.controlling_area = src.controlling_area,
            tgt.hierarchy_name = src.hierarchy_name,
            tgt.hierarchy_level = src.hierarchy_level,
            tgt.is_leaf_node = src.is_leaf_node,
            tgt.level_1_node = src.level_1_node,
            tgt.level_1_node_text = src.level_1_node_text,
            tgt.level_1_node_sort_order = src.level_1_node_sort_order,
            tgt.level_2_node = src.level_2_node,
            tgt.level_2_node_text = src.level_2_node_text,
            tgt.level_2_node_sort_order = src.level_2_node_sort_order,
            tgt.level_3_node = src.level_3_node,
            tgt.level_3_node_text = src.level_3_node_text,
            tgt.level_3_node_sort_order = src.level_3_node_sort_order,
            tgt.level_4_node = src.level_4_node,
            tgt.level_4_node_text = src.level_4_node_text,
            tgt.level_4_node_sort_order = src.level_4_node_sort_order,
            tgt.level_5_node = src.level_5_node,
            tgt.level_5_node_text = src.level_5_node_text,
            tgt.level_5_node_sort_order = src.level_5_node_sort_order,
            tgt.level_6_node = src.level_6_node,
            tgt.level_6_node_text = src.level_6_node_text,
            tgt.level_6_node_sort_order = src.level_6_node_sort_order,
            tgt.level_7_node = src.level_7_node,
            tgt.level_7_node_text = src.level_7_node_text,
            tgt.level_7_node_sort_order = src.level_7_node_sort_order,
            tgt.level_8_node = src.level_8_node,
            tgt.level_8_node_text = src.level_8_node_text,
            tgt.level_8_node_sort_order = src.level_8_node_sort_order,
            tgt.level_9_node = src.level_9_node,
            tgt.level_9_node_text = src.level_9_node_text,
            tgt.level_9_node_sort_order = src.level_9_node_sort_order
    WHEN NOT MATCHED THEN
        INSERT (
            hierarchy_id,
            cost_center_hierarchy_node,
            controlling_area,
            hierarchy_name,
            hierarchy_level,
            is_leaf_node,
            level_1_node,
            level_1_node_text,
            level_1_node_sort_order,
            level_2_node,
            level_2_node_text,
            level_2_node_sort_order,
            level_3_node,
            level_3_node_text,
            level_3_node_sort_order,
            level_4_node,
            level_4_node_text,
            level_4_node_sort_order,
            level_5_node,
            level_5_node_text,
            level_5_node_sort_order,
            level_6_node,
            level_6_node_text,
            level_6_node_sort_order,
            level_7_node,
            level_7_node_text,
            level_7_node_sort_order,
            level_8_node,
            level_8_node_text,
            level_8_node_sort_order,
            level_9_node,
            level_9_node_text,
            level_9_node_sort_order
        ) VALUES (
            src.hierarchy_id,
            src.cost_center_hierarchy_node,
            src.controlling_area,
            src.hierarchy_name,
            src.hierarchy_level,
            src.is_leaf_node,
            src.level_1_node,
            src.level_1_node_text,
            src.level_1_node_sort_order,
            src.level_2_node,
            src.level_2_node_text,
            src.level_2_node_sort_order,
            src.level_3_node,
            src.level_3_node_text,
            src.level_3_node_sort_order,
            src.level_4_node,
            src.level_4_node_text,
            src.level_4_node_sort_order,
            src.level_5_node,
            src.level_5_node_text,
            src.level_5_node_sort_order,
            src.level_6_node,
            src.level_6_node_text,
            src.level_6_node_sort_order,
            src.level_7_node,
            src.level_7_node_text,
            src.level_7_node_sort_order,
            src.level_8_node,
            src.level_8_node_text,
            src.level_8_node_sort_order,
            src.level_9_node,
            src.level_9_node_text,
            src.level_9_node_sort_order
        );
''')

# COMMAND ----------

logger.log.info(f'Merge: {dest_tablename} {merge_result.toPandas().head(1)}')