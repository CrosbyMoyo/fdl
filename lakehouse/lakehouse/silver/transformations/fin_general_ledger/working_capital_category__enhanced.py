# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.working_capital_category.yaml"
logger.log.info(f'"metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

source_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("consolidation_reporting_item_hierarchy")}'

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver")}'

# COMMAND ----------

level_1_nodes = spark.sql(f'''
    SELECT DISTINCT 
        CASE
            WHEN v.level_1_node = 'ZWC' THEN 9 --
            ELSE 9999
        END AS rank,
        trim(v.level_1_node) AS parent_id,
        v.reporting_item_hierarchy_node AS wc_category_node,
        trim(v.level_1_node_text) as wc_category
    FROM
        {source_tablename} AS v
    WHERE
        v.hierarchy_id = 'CS16/C1/ZWC' --AND v.is_leaf_node = True
        AND v.level_1_node IN ('ZWC')
        AND v.hierarchy_level = 1                     
''')

level_1_nodes.createOrReplaceTempView('level_1_nodes')

# COMMAND ----------

level_2_nodes = spark.sql(f'''
    SELECT DISTINCT 
            CASE
            WHEN v.level_2_node = 'WC100' THEN 1 --
            WHEN v.level_2_node = 'WC200' THEN 2 --
            WHEN v.level_2_node = 'WC300' THEN 3 --
            WHEN v.level_2_node = 'WC400' THEN 4 --
            WHEN v.level_2_node = 'WC500' THEN 5 --
            WHEN v.level_2_node = 'WC600' THEN 6 --
            WHEN v.level_2_node = 'WC700' THEN 7 --
            WHEN v.level_2_node = 'WC800' THEN 8 --
            ELSE 9999
        END AS rank,
        trim(v.level_2_node) AS parent_id,
        v.reporting_item_hierarchy_node AS wc_category_node,
        trim(v.level_2_node_text) as wc_category
    FROM
        {source_tablename} AS v
    WHERE
        v.hierarchy_id = 'CS16/C1/ZWC' --AND v.is_leaf_node = True
        AND v.level_2_node IN (
            'WC100',
            'WC200',
            'WC300',
            'WC400',
            'WC500',
            'WC600',
            'WC700',
            'WC800'
        )
        AND v.hierarchy_level = 2                       
''')

level_2_nodes.createOrReplaceTempView('level_2_nodes')

# COMMAND ----------

enhanced = spark.sql(f'''
        SELECT 
            lvl1.*
        FROM level_1_nodes AS lvl1
    UNION ALL
        SELECT
            lvl2.*
        FROM level_2_nodes AS lvl2
''')
enhanced.createOrReplaceTempView('enhanced')

# COMMAND ----------

write_result = metadata.process_transformation_table('enhanced', env_vars.silver_catalog)
logger.log.info(f'Write: {dest_tablename} {write_result}')