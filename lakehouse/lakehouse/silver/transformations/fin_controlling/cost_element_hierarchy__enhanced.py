# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.cost_element_hierarchy.yaml"
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

# get the columns to xxhash64
row_fprint_ddl = metadata.get_payload_columns_ddl()
# get the columns to MATCH
match_cols_ddl = metadata.get_update_set_ddl("src.", "tgt.")
# get the target columns to INSERT
insert_cols_tgt_ddl = metadata.get_insert_ddl()
# get the source columns to INSERT
insert_cols_src_ddl = metadata.get_insert_ddl("src.")

# COMMAND ----------

source_tablename = f'{env_vars.silver_catalog}.{metadata.source_2partname(include_schemaversion=True)}'

# COMMAND ----------

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", include_schemaversion=True)}'

# COMMAND ----------

spark.sql(
    f"""
        SELECT
            s.sub_key_id              AS controlling_area
            ,s.hier_id                AS hierarchy_id
            ,s.level1_text            AS hierarchy_name
            ,s.node_id                AS cost_element_hierarchy_node
            ,s.level                  AS hierarchy_level
            ,s.leaf_flag              AS is_leaf_node
            ,s.level2_node            AS level_1_node
            ,s.level2_text            AS level_1_node_text
            ,s.level2_seqnr           AS level_1_node_sort_order
            ,s.level3_node            AS level_2_node
            ,s.level3_text            AS level_2_node_text
            ,s.level3_seqnr           AS level_2_node_sort_order
            ,s.level4_node            AS level_3_node
            ,s.level4_text            AS level_3_node_text
            ,s.level4_seqnr           AS level_3_node_sort_order
            ,s.level5_node            AS level_4_node
            ,s.level5_text            AS level_4_node_text
            ,s.level5_seqnr           AS level_4_node_sort_order
            ,s.__etl_keys_fprint
            ,xxhash64({row_fprint_ddl}) AS __etl_row_fprint
            ,s.__etl_effective_from
            ,s.__etl_effective_to
            ,s.__etl_is_active
            ,s.__etl_is_deleted
        FROM
            {source_tablename} AS s
    """
).createOrReplaceTempView('enhanced')

# COMMAND ----------

write_result = spark.sql(f'''
    INSERT OVERWRITE TABLE {dest_tablename}
    BY NAME
    SELECT
        {insert_cols_src_ddl}
    FROM
        enhanced AS src;
''')

# COMMAND ----------

logger.log.info(f'Merge: {dest_tablename} {write_result.toPandas().head(1)}')