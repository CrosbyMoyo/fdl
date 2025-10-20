# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.leveraged_free_cash_flow_subcategory.yaml"
logger.log.info(f'"metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

reporting_item_hierarchy_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("consolidation_reporting_item_hierarchy")}'

leveraged_free_cash_flow_category_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("leveraged_free_cash_flow_category")}'

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver")}'

# COMMAND ----------

base = spark.sql(f'''
    SELECT
        ri.reporting_item_hierarchy_node AS node,
        ri.reporting_item_description AS sub_category_description,
        /* build arrays of codes & texts for fast ancestor search */
        array(
            ri.level_1_node,
            ri.level_2_node,
            ri.level_3_node,
            ri.level_4_node,
            ri.level_5_node,
            ri.level_6_node,
            ri.level_7_node,
            ri.level_8_node
        ) AS lineage,
        array(
            ri.level_1_node_text,
            ri.level_2_node_text,
            ri.level_3_node_text,
            ri.level_4_node_text,
            ri.level_5_node_text,
            ri.level_6_node_text,
            ri.level_7_node_text,
            ri.level_8_node_text
        ) AS lineage_txt
    FROM
        {reporting_item_hierarchy_tablename} AS ri
    WHERE
        ri.hierarchy_id = 'CS16/C1/FCF00'        
''')

base.createOrReplaceTempView('base')

# COMMAND ----------

enhanced = spark.sql(f'''
    SELECT
        b.node,
        b.sub_category_description,
        c.parent_id AS category_id,
        c.lfcf_category AS category_description,
        c.rank AS category_rank
    FROM
        base AS b
        JOIN {leveraged_free_cash_flow_category_tablename} AS c
            ON array_contains(b.lineage, c.parent_id)                     
''')

enhanced.createOrReplaceTempView('enhanced')

# COMMAND ----------

write_result = metadata.process_transformation_table('enhanced', env_vars.silver_catalog)
logger.log.info(f'Write: {dest_tablename} {write_result}')