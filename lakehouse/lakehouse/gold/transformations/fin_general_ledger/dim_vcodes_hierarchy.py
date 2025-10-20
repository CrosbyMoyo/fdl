# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')


# COMMAND ----------

dest_tablename = f'{env_vars.gold_catalog}.fin_general_ledger.dim_vcodes_hierarchy'

# COMMAND ----------

spark.sql(
    f'''
        SELECT 
            p.*
        from 
            {env_vars.silver_catalog}.fin_general_ledger.vcodes_hierarchy AS p
    '''
).createOrReplaceTempView('hashed')

# COMMAND ----------

merge_result = spark.sql(f'''
    MERGE INTO {dest_tablename} AS tgt
    USING hashed AS src
        ON tgt.hierarchy_id = src.hierarchy_id AND 
        tgt.vcode_hierarchy_node = src.vcode_hierarchy_node  
    WHEN MATCHED THEN
        UPDATE SET 
            tgt.vcode_description = src.vcode_description,
            tgt.hierarchy_name = src.hierarchy_name,
            tgt.hierarchy_level = src.hierarchy_level,
            tgt.is_leaf_node = src.is_leaf_node,
            tgt.level_1_node = src.level_1_node,
            tgt.level_1_node_text = src.level_1_node_text,
            tgt.level_2_node = src.level_2_node,
            tgt.level_2_node_text = src.level_2_node_text,
            tgt.level_3_node = src.level_3_node,
            tgt.level_3_node_text = src.level_3_node_text,
            tgt.level_4_node = src.level_4_node,
            tgt.level_4_node_text = src.level_4_node_text,
            tgt.level_5_node = src.level_5_node,
            tgt.level_5_node_text = src.level_5_node_text,
            tgt.level_6_node = src.level_6_node,
            tgt.level_6_node_text = src.level_6_node_text,
            tgt.level_7_node = src.level_7_node,
            tgt.level_7_node_text = src.level_7_node_text,
            tgt.level_8_node = src.level_8_node,
            tgt.level_8_node_text = src.level_8_node_text,
            tgt.level_9_node = src.level_9_node,
            tgt.level_9_node_text = src.level_9_node_text,
            tgt.level_10_node = src.level_10_node,
            tgt.level_10_node_text = src.level_10_node_text,
            tgt.level_11_node = src.level_11_node,
            tgt.level_11_node_text = src.level_11_node_text,
            tgt.level_12_node = src.level_12_node,
            tgt.level_12_node_text = src.level_12_node_text,
            tgt.level_13_node = src.level_13_node,
            tgt.level_13_node_text = src.level_13_node_text,
            tgt.level_14_node = src.level_14_node,
            tgt.level_14_node_text = src.level_14_node_text
    WHEN NOT MATCHED THEN
        INSERT (
            hierarchy_id,
            vcode_hierarchy_node,
            vcode_description,
            hierarchy_name,
            hierarchy_level,
            is_leaf_node,
            level_1_node,
            level_1_node_text,
            level_2_node,
            level_2_node_text,
            level_3_node,
            level_3_node_text,
            level_4_node,
            level_4_node_text,
            level_5_node,
            level_5_node_text,
            level_6_node,
            level_6_node_text,
            level_7_node,
            level_7_node_text,
            level_8_node,
            level_8_node_text,
            level_9_node,
            level_9_node_text,
            level_10_node,
            level_10_node_text,
            level_11_node,
            level_11_node_text,
            level_12_node,
            level_12_node_text,
            level_13_node,
            level_13_node_text,
            level_14_node,
            level_14_node_text
        ) VALUES (
            src.hierarchy_id,
            src.vcode_hierarchy_node,
            src.vcode_description,
            src.hierarchy_name,
            src.hierarchy_level,
            src.is_leaf_node,
            src.level_1_node,
            src.level_1_node_text,
            src.level_2_node,
            src.level_2_node_text,
            src.level_3_node,
            src.level_3_node_text,
            src.level_4_node,
            src.level_4_node_text,
            src.level_5_node,
            src.level_5_node_text,
            src.level_6_node,
            src.level_6_node_text,
            src.level_7_node,
            src.level_7_node_text,
            src.level_8_node,
            src.level_8_node_text,
            src.level_9_node,
            src.level_9_node_text,
            src.level_10_node,
            src.level_10_node_text,
            src.level_11_node,
            src.level_11_node_text,
            src.level_12_node,
            src.level_12_node_text,
            src.level_13_node,
            src.level_13_node_text,
            src.level_14_node,
            src.level_14_node_text
        );
''')

# COMMAND ----------

logger.log.info(f'Merge: {dest_tablename} {merge_result.toPandas().head(1)}')