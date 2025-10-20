# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.trial_balance_subcategory.yaml"
logger.log.info(f'"metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

financial_statement_item_hierarchy_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("financial_statement_item_hierarchy", include_schemaversion=True)}'

trial_balance_assets_category_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("trial_balance_assets_category", include_schemaversion=True)}'

trial_balance_eq_liab_category_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("trial_balance_eq_liab_category", include_schemaversion=True)}'

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", include_schemaversion=True)}'

# COMMAND ----------

base = spark.sql(f'''
    SELECT
        fsi.financial_statement_item_hierarchy_node AS node,
        fsi.financial_statement_item_description AS sub_category_description,
        /* build arrays of codes & texts for fast ancestor search */
        array(
            fsi.level_1_node,
            fsi.level_2_node,
            fsi.level_3_node,
            fsi.level_4_node,
            fsi.level_5_node,
            fsi.level_6_node,
            fsi.level_7_node,
            fsi.level_8_node,
            fsi.level_9_node
        ) AS lineage,
        array(
            fsi.level_1_node_text,
            fsi.level_2_node_text,
            fsi.level_3_node_text,
            fsi.level_4_node_text,
            fsi.level_5_node_text,
            fsi.level_6_node_text,
            fsi.level_7_node_text,
            fsi.level_8_node_text,
            fsi.level_9_node_text
        ) AS lineage_txt
    FROM
        {financial_statement_item_hierarchy_tablename} AS fsi
    WHERE
        fsi.hierarchy_id = 'CS15/C1/ZBS_PL'        
''')

base.createOrReplaceTempView('base')

# COMMAND ----------

trial_balance_assets_category = spark.sql(f'''
    SELECT
        b.node,
        b.sub_category_description,
        c.parent_id AS category_id,
        c.tb_category AS category_description,
        c.rank AS category_rank
    FROM
        base AS b
    JOIN {trial_balance_assets_category_tablename} AS c
        ON array_contains(b.lineage, c.parent_id)             
''')

trial_balance_assets_category.createOrReplaceTempView('trial_balance_assets_category')

# COMMAND ----------

trial_balance_eq_liab_category = spark.sql(f'''
    SELECT
        b.node,
        b.sub_category_description,
        c.parent_id AS category_id,
        c.tb_category AS category_description,
        c.rank AS category_rank
    FROM
        base AS b
    JOIN {trial_balance_eq_liab_category_tablename} AS c
        ON array_contains(b.lineage, c.parent_id)                                
''')

trial_balance_eq_liab_category.createOrReplaceTempView('trial_balance_eq_liab_category')

# COMMAND ----------

enhanced = spark.sql(f'''
        SELECT DISTINCT
            tba.*
        FROM trial_balance_assets_category AS tba
    UNION ALL
        SELECT DISTINCT
            tbeq.*
        FROM trial_balance_eq_liab_category AS tbeq
''')

enhanced.createOrReplaceTempView('enhanced')

# COMMAND ----------

write_result = metadata.process_transformation_table('enhanced', env_vars.silver_catalog)
logger.log.info(f'Write: {dest_tablename} {write_result}')