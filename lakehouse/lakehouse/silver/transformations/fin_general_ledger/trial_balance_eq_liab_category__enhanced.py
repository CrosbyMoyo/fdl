# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.trial_balance_eq_liab_category.yaml"
logger.log.info(f'"metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

financial_statement_item_hierarchy_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("financial_statement_item_hierarchy", include_schemaversion=True)}'

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", include_schemaversion=True)}'

# COMMAND ----------

level_4_nodes = spark.sql(f'''
    SELECT
        DISTINCT CASE
            WHEN v.level_4_node = '62000' THEN 1 -- Total equity
        END AS rank,
        trim(v.level_4_node) AS parent_id,
        v.financial_statement_item_hierarchy_node AS tb_category_node,
        trim(v.level_4_node_text) AS tb_category,
        'EL' AS report_category
    FROM
        {financial_statement_item_hierarchy_tablename} AS v
    WHERE
        v.hierarchy_id = 'CS15/C1/ZBS_PL'
        AND v.level_4_node IN ('62000')
        AND v.hierarchy_level = '4'                          
''')

level_4_nodes.createOrReplaceTempView('level_4_nodes')

# COMMAND ----------

level_5_nodes = spark.sql(f'''
    SELECT
        DISTINCT CASE
            WHEN v.level_5_node = 'LGAAP' THEN 14
        END AS rank,
        trim(v.level_5_node) AS parent_id,
        v.financial_statement_item_hierarchy_node AS tb_category_node,
        trim(v.level_5_node_text) as tb_category,
        'EL' as report_category
    FROM
        {financial_statement_item_hierarchy_tablename} AS v
    WHERE
        v.hierarchy_id = 'CS15/C1/ZBS_PL'
        AND v.level_5_node IN ('LGAAP')
        AND v.hierarchy_level = '5'                                                
''')

level_5_nodes.createOrReplaceTempView('level_5_nodes')

# COMMAND ----------

level_6_nodes = spark.sql(f'''
    SELECT
        DISTINCT CASE
            WHEN v.level_6_node = '65060' THEN 2 -- Lease liabilities (non-current)
            WHEN v.level_6_node = '65010' THEN 3 -- Borrowings (Non-Current)
            WHEN v.level_6_node = '65040' THEN 4 -- Provisions (non-current)
            WHEN v.level_6_node = '65050' THEN 5 -- Deferred tax liabilities
            WHEN v.level_6_node = '65030' THEN 6 -- Other liabilities (non-current)
            WHEN v.level_6_node = '61060' THEN 7 -- Lease liability (current)
            WHEN v.level_6_node = '61050' THEN 8 -- Trade and other payables
            WHEN v.level_6_node = '61010' THEN 9 -- Borrowings (current)
            WHEN v.level_6_node = '61040' THEN 10 -- Provisions (current)
            WHEN v.level_6_node = '61020' THEN 11 -- Other financial liabilities (current)
            WHEN v.level_6_node = '61030_1' THEN 12 -- Other liabilities (current)
            WHEN v.level_6_node = '9400' THEN 13 -- Current tax liabilities
        END AS rank,
        trim(v.level_6_node) AS parent_id,
        v.financial_statement_item_hierarchy_node AS tb_category_node,
        trim(v.level_6_node_text) as tb_category,
        'EL' as report_category
    FROM
        {financial_statement_item_hierarchy_tablename} AS v
    WHERE
        v.hierarchy_id = 'CS15/C1/ZBS_PL' --AND v.is_leaf_node = True
        AND v.level_6_node IN (
            '65060', 
            '65010', 
            '65040', 
            '65050', 
            '65030',
            '61060',
            '61050',
            '61010',
            '61040',
            '61020',
            '61030_1',
            '9400'
        )
        AND v.hierarchy_level = '6'                  
''')

level_6_nodes.createOrReplaceTempView('level_6_nodes')

# COMMAND ----------

enhanced = spark.sql(f'''
        SELECT
            lvl4.*
        FROM level_4_nodes AS lvl4
    UNION ALL
        SELECT
            lvl5.*
        FROM level_5_nodes AS lvl5
    UNION ALL
        SELECT
            lvl6.*
        FROM level_6_nodes AS lvl6                  
''')

enhanced.createOrReplaceTempView('enhanced')

# COMMAND ----------

write_result = metadata.process_transformation_table('enhanced', env_vars.silver_catalog)
logger.log.info(f'Write: {dest_tablename} {write_result}')