# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.financial_statement_version_hierarchy.yaml"
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
             s.hierarchy_id
            ,s.hierarchy_name
            ,s.node_id                AS gl_account
            ,s.level                  AS hierarchy_level
            ,s.chart_of_accounts
            ,s.leaf_flag              AS is_leaf_node
            ,s.level_1_node
            ,s.level_1_node_text
            ,s.level_2_node
            ,s.level_2_node_text
            ,s.level_3_node
            ,s.level_3_node_text
            ,s.level_4_node
            ,s.level_4_node_text
            ,s.level_5_node
            ,s.level_5_node_text
            ,s.level_6_node
            ,s.level_6_node_text
            ,s.level_7_node
            ,s.level_7_node_text
            ,s.level_8_node
            ,s.level_8_node_text
            ,s.level_9_node
            ,s.level_9_node_text
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

merge_result = spark.sql(f'''
    MERGE INTO {dest_tablename} AS tgt
    USING enhanced AS src
        ON tgt.__etl_keys_fprint = src.__etl_keys_fprint
    WHEN MATCHED THEN
        UPDATE SET
            {match_cols_ddl},
            tgt.__etl_row_fprint = src.__etl_row_fprint,
            tgt.__etl_effective_to = src.__etl_effective_from,
            tgt.__etl_is_active = src.__etl_is_active,
            tgt.__etl_is_deleted = src.__etl_is_deleted
    WHEN NOT MATCHED THEN
        INSERT (
            {insert_cols_tgt_ddl}
        )
        VALUES (
            {insert_cols_src_ddl}
        );
''')

# COMMAND ----------

logger.log.info(f'Merge: {dest_tablename} {merge_result.toPandas().head(1)}')