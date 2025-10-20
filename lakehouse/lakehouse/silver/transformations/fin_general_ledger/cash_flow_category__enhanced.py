# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.cash_flow_category.yaml"
logger.log.info(f'"metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

source_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("consolidation_reporting_item_hierarchy", include_schemaversion=True)}'

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", include_schemaversion=True)}'

# COMMAND ----------

level_2_nodes = spark.sql(f'''
    SELECT
        DISTINCT CASE
            WHEN v.level_2_node = 'CF0000' THEN 31 -- Cashflow Balancing Check
            WHEN v.level_2_node = 'CF1420' THEN 32 -- Cash and cash equivalents, opening from BS
            WHEN v.level_2_node = 'CF1430' THEN 33 -- Cash and cash equivalents, closing from BS
            ELSE 9999
        END AS rank,
        trim(v.level_2_node) AS parent_id,
        v.reporting_item_hierarchy_node AS cf_category_node,
        trim(v.level_2_node_text) as cf_category
    FROM
        {source_tablename} AS v
    WHERE
        v.hierarchy_id = 'CS16/C1/ZCF'
        AND v.level_2_node IN (
            'CF0000', 
            'CF1420', 
            'CF1430'
        )
        AND v.hierarchy_level = 2                          
''')

level_2_nodes.createOrReplaceTempView('level_2_nodes')

# COMMAND ----------

level_3_nodes = spark.sql(f'''
    SELECT
        DISTINCT CASE
            WHEN v.level_3_node = 'CF1000' THEN 29 -- Net Cash flow
            WHEN v.level_3_node = 'CF1500' THEN 30 -- Cash and cash equivalents, movement
            ELSE 9999
        END AS rank,
        trim(v.level_3_node) AS parent_id,
        v.reporting_item_hierarchy_node AS cf_category_node,
        '...' || trim(v.level_3_node_text) as cf_category
    FROM
        {source_tablename} AS v
    WHERE
        v.hierarchy_id = 'CS16/C1/ZCF'
        AND v.level_3_node IN (
            'CF1000', 
            'CF1500'
        )
        AND v.hierarchy_level = 3                          
''')

level_3_nodes.createOrReplaceTempView('level_3_nodes')

# COMMAND ----------

level_4_nodes = spark.sql(f'''
    SELECT
        DISTINCT CASE
            WHEN v.level_4_node = 'CF1100' THEN 12 -- Cash flows from operating activities
            WHEN v.level_4_node = 'CF1200' THEN 17 -- Cash flow from investment activities
            WHEN v.level_4_node = 'CF1300' THEN 27 -- Cash flows from financing activities
            WHEN v.level_4_node = 'CF1370' THEN 28 -- Exchange gain/losses on cash and cash equivalents
            ELSE 9999
        END AS rank,
        trim(v.level_4_node) AS parent_id,
        v.reporting_item_hierarchy_node AS cf_category_node,
        '......' || trim(v.level_4_node_text) as cf_category
    FROM
        {source_tablename} AS v
    WHERE
        v.hierarchy_id = 'CS16/C1/ZCF'
        AND v.level_4_node IN (
            'CF1100',
            'CF1200',
            'CF1300',
            'CF1370'
        )
        AND v.hierarchy_level = 4                         
''')

level_4_nodes.createOrReplaceTempView("level_4_nodes")

# COMMAND ----------

level_5_nodes = spark.sql(f'''
    SELECT
        DISTINCT CASE
            WHEN v.level_5_node = 'CF1110' THEN 1 -- Net income
            WHEN v.level_5_node = 'CF1111' THEN 11 -- Adjustment For
            WHEN v.level_5_node = 'CF1210' THEN 13 -- Acquisition of businesses, net of cash acquired
            WHEN v.level_5_node = 'CF1230' THEN 14 -- Purchases of PPE and intangible assets
            WHEN v.level_5_node = 'CF1240' THEN 15 -- Proceeds from disposals of PPE & intangible assets
            WHEN v.level_5_node = 'CF1249' THEN 16 -- Other investment activities
            WHEN v.level_5_node = 'CF1310' THEN 18 -- Proceeds from issuance of share (premium)
            WHEN v.level_5_node = 'CF1321' THEN 19 -- Repayment of long term debt
            WHEN v.level_5_node = 'CF1322' THEN 20 -- Proceeds from shareholder loan
            WHEN v.level_5_node = 'CF1330' THEN 21 -- Repayment of shareholder loan
            WHEN v.level_5_node = 'CF1340' THEN 22 -- Proceeds from bank and other borrowings
            WHEN v.level_5_node = 'CF1341' THEN 23 -- Repayment of lease liability
            WHEN v.level_5_node = 'CF1345' THEN 24 -- Dividends received / paid
            WHEN v.level_5_node = 'CF1350' THEN 25 -- Interest paid
            WHEN v.level_5_node = 'CF1355' THEN 26 -- Interest from cash and cash equivalents
            ELSE 9999
        END AS rank,
        trim(v.level_5_node) AS parent_id,
        v.reporting_item_hierarchy_node AS cf_category_node,
        '.........' || trim(v.level_5_node_text) as cf_category
    FROM
        {source_tablename} AS v
    WHERE
        v.hierarchy_id = 'CS16/C1/ZCF'
        AND v.level_5_node IN (
            'CF1110',
            'CF1111',
            'CF1210',
            'CF1230',
            'CF1240',
            'CF1249',
            'CF1310',
            'CF1321',
            'CF1322',
            'CF1330',
            'CF1340',
            'CF1341',
            'CF1345',
            'CF1350',
            'CF1355'
        )
        AND v.hierarchy_level = 5
''')

level_5_nodes.createOrReplaceTempView("level_5_nodes")

# COMMAND ----------

level_6_nodes = spark.sql(f'''
    SELECT
        DISTINCT CASE
            WHEN v.level_6_node = 'CF1120' THEN 2 -- Income taxes
            WHEN v.level_6_node = 'CF1121' THEN 3 -- Amortisation and depreciation
            WHEN v.level_6_node = 'CF1123' THEN 4 -- Net gains on disposal of PPE and intangible assets
            WHEN v.level_6_node = 'CF1124' THEN 5 -- Share of profit of joint ventures
            WHEN v.level_6_node = 'CF1125' THEN 6 -- Dividends received frm joint ventures & associates
            WHEN v.level_6_node = 'CF1126' THEN 7 -- Decrease/(increase) in inventories
            WHEN v.level_6_node = 'CF1127' THEN 8 -- Decrease/(increase) in trade receivables
            WHEN v.level_6_node = 'CF1140' THEN 9 -- Current income taxes paid
            WHEN v.level_6_node = 'CF1149' THEN 10 -- Other operating activities
            ELSE 9999
        END AS rank,
        trim(v.level_6_node) AS parent_id,
        v.reporting_item_hierarchy_node AS cf_category_node,
        '............' || trim(v.level_6_node_text) as cf_category
    FROM
        {source_tablename} AS v
    WHERE
        v.hierarchy_id = 'CS16/C1/ZCF'
        AND v.level_6_node IN (
            'CF1120',
            'CF1121',
            'CF1123',
            'CF1124',
            'CF1125',
            'CF1126',
            'CF1127',
            'CF1140',
            'CF1149'
        )
        AND v.hierarchy_level = 6                       
''')

level_6_nodes.createOrReplaceTempView("level_6_nodes")

# COMMAND ----------

enhanced = spark.sql(f'''
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