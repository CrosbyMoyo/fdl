# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.trial_balance_assets_category.yaml"
logger.log.info(f'"metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

financial_statement_item_hierarchy_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("financial_statement_item_hierarchy", include_schemaversion=True)}'

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", include_schemaversion=True)}'

# COMMAND ----------

enhanced = spark.sql(f'''
    SELECT
        DISTINCT CASE
            WHEN v.level_5_node = '64060' THEN 1
            WHEN v.level_5_node = '64050' THEN 2
            WHEN v.level_5_node = '64020' THEN 3
            WHEN v.level_5_node = '64030' THEN 4
            WHEN v.level_5_node = '64010' THEN 5
            WHEN v.level_5_node = '60040_1' THEN 6
            WHEN v.level_5_node = '60030_1' THEN 7
            WHEN v.level_5_node = '60020' THEN 8
            WHEN v.level_5_node = '60050' THEN 9
            WHEN v.level_5_node = '6300' THEN 10
            ELSE 9999
        END AS rank,
        trim(v.level_5_node) AS parent_id,
        v.financial_statement_item_hierarchy_node AS tb_category_node,
        trim(v.level_5_node_text) as tb_category,
        'Assets' as report_category
    FROM
        {financial_statement_item_hierarchy_tablename} AS v
    WHERE
        v.hierarchy_id = 'CS15/C1/ZBS_PL'
        AND v.level_5_node IN (
            '64060',
            '64050',
            '64020',
            '64030',
            '64010',
            '60040_1',
            '60030_1',
            '60020',
            '60050',
            '6300'
        )
        AND v.hierarchy_level = '5'                    
''')

enhanced.createOrReplaceTempView('enhanced')

# COMMAND ----------

write_result = metadata.process_transformation_table('enhanced', env_vars.silver_catalog)
logger.log.info(f'Write: {dest_tablename} {write_result}')