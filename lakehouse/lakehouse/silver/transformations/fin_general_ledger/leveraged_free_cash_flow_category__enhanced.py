# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.leveraged_free_cash_flow_category.yaml"
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
            WHEN v.level_1_node = 'FCF00' THEN 15 -- Free Cashflow
            ELSE 9999
        END AS rank,
        trim(v.level_1_node) AS parent_id,
        v.reporting_item_hierarchy_node AS lfcf_category_node,
        trim(v.level_1_node_text) as lfcf_category
    FROM
        {source_tablename} AS v
    WHERE
        v.hierarchy_id = 'CS16/C1/FCF00'
        AND v.level_1_node IN ('FCF00')
        AND v.hierarchy_level = 1                         
''')

level_1_nodes.createOrReplaceTempView('level_1_nodes')

# COMMAND ----------

level_2_nodes = spark.sql(f'''
    SELECT DISTINCT 
        CASE
            WHEN v.level_2_node = 'FCF2' THEN 14 -- Levered FCF (after WC)
            ELSE 9999
        END AS rank,
        trim(v.level_2_node) AS parent_id,
        v.reporting_item_hierarchy_node AS lfcf_category_node,
        trim(v.level_2_node_text) as lfcf_category
    FROM
        {source_tablename} AS v
    WHERE
        v.hierarchy_id = 'CS16/C1/FCF00'
        AND v.level_2_node IN ('FCF2')
        AND v.hierarchy_level = 2                        
''')

level_2_nodes.createOrReplaceTempView('level_2_nodes')

# COMMAND ----------

level_3_nodes = spark.sql(f'''
    SELECT DISTINCT 
        CASE
            WHEN v.level_3_node = 'FCF1' THEN 8 -- Levered FCF (pre-WC change)
            WHEN v.level_3_node = 'FCF80' THEN 12 -- Change in core working capital
            WHEN v.level_3_node = 'FCF90' THEN 13 -- Change in other operating activities
            ELSE 9999
        END AS rank,
        trim(v.level_3_node) AS parent_id,
        v.reporting_item_hierarchy_node AS lfcf_category_node,
        trim(v.level_3_node_text) as lfcf_category
    FROM
        {source_tablename} AS v
    WHERE
        v.hierarchy_id = 'CS16/C1/FCF00' --AND v.is_leaf_node = True
        AND v.level_3_node IN ('FCF1', 'FCF80', 'FCF90')
        AND v.hierarchy_level = 3                       
''')

level_3_nodes.createOrReplaceTempView('level_3_nodes')

# COMMAND ----------

level_4_nodes = spark.sql(f'''
    SELECT DISTINCT 
        CASE
            WHEN v.level_4_node = 'FCF10' THEN 1 -- EBITDA
            WHEN v.level_4_node = 'FCF20' THEN 2 -- Tax paid
            WHEN v.level_4_node = 'FCF30' THEN 3 -- Interest paid
            WHEN v.level_4_node = 'FCF40' THEN 4 -- Lease principle repayment
            WHEN v.level_4_node = 'FCF50' THEN 5 -- Delta in JV
            WHEN v.level_4_node = 'FCF60' THEN 6 -- Net capex
            WHEN v.level_4_node = 'FCF70' THEN 7 -- Business acquisition
            WHEN v.level_4_node = 'FCF81' THEN 9 -- Trade Payables
            WHEN v.level_4_node = 'FCF82' THEN 10 -- Trade Receivables
            WHEN v.level_4_node = 'FCF83' THEN 11 -- Inventories
            ELSE 9999
        END AS rank,
        trim(v.level_4_node) AS parent_id,
        v.reporting_item_hierarchy_node AS lfcf_category_node,
        trim(v.level_4_node_text) as lfcf_category
    FROM
        {source_tablename} AS v
    WHERE
        v.hierarchy_id = 'CS16/C1/FCF00' --AND v.is_leaf_node = True
        AND v.level_4_node IN (
            'FCF10',
            'FCF20',
            'FCF30',
            'FCF40',
            'FCF50',
            'FCF60',
            'FCF70',
            'FCF81',
            'FCF82',
            'FCF83'
        )
        AND v.hierarchy_level = 4                        
''')

level_4_nodes.createOrReplaceTempView("level_4_nodes")

# COMMAND ----------

enhanced = spark.sql(f'''
        SELECT 
            lvl1.*
        FROM level_1_nodes AS lvl1
    UNION ALL
        SELECT
            lvl2.*
        FROM level_2_nodes AS lvl2
    UNION ALL
        SELECT
            lvl3.*
        FROM level_3_nodes AS lvl3
    UNION ALL
        SELECT
            lvl4.*
        FROM level_4_nodes AS lvl4
''')
enhanced.createOrReplaceTempView('enhanced')

# COMMAND ----------

write_result = metadata.process_transformation_table('enhanced', env_vars.silver_catalog)
logger.log.info(f'Write: {dest_tablename} {write_result}')