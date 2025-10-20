# Databricks notebook source
# MAGIC %md
# MAGIC ## fagl_011zc Bronze to Silver
# MAGIC
# MAGIC Generic Notebook moves the `{bronze}.fivetran_s4p.fagl_011zc` data to a staging table defined in the metadata.
# MAGIC
# MAGIC This notebook further refines that data, and adds the payload fingerprint.  Then merges the data into `{silver}.fin_general_ledger.financial_statement_assignment_item_gl` table.

# COMMAND ----------

# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.fagl_011zc.yaml"
logger.log.info(f'"metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

source_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("casted", include_schemaversion=True)}'

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", include_schemaversion=True)}'

# COMMAND ----------

# get the columns to xxhash64
row_fprint_ddl = metadata.get_payload_columns_ddl("source.")
# get the columns to MATCH
match_cols_ddl = metadata.get_update_set_ddl("source.", "target.")
# get the target columns to INSERT
insert_cols_tgt_ddl = metadata.get_insert_ddl()
# get the source columns to INSERT
insert_cols_src_ddl = metadata.get_insert_ddl("source.")

# COMMAND ----------

am_enhanced = spark.sql(f'''
    SELECT DISTINCT
        -- key
        source.client,
        source.financial_statement_version,
        source.financial_statement_item,
        source.chart_of_accounts,
        source.account_from,

        --payload
        source.account_to,

        --metadata
        source.__etl_keys_fprint,
        xxhash64({row_fprint_ddl}) AS __etl_row_fprint,
        source.__etl_effective_from,
        source.__etl_effective_to,
        source.__etl_is_active,
        source.__etl_is_deleted
    FROM
        {source_tablename} AS source;                
''').createOrReplaceTempView("am_enhanced")

# COMMAND ----------

merge_result = spark.sql(f'''
    MERGE INTO {dest_tablename} AS target
    USING am_enhanced AS source
        ON source.__etl_keys_fprint = target.__etl_keys_fprint
    WHEN MATCHED THEN
        UPDATE SET
            {match_cols_ddl},
            target.__etl_row_fprint = source.__etl_row_fprint,
            target.__etl_effective_from = source.__etl_effective_from,
            target.__etl_effective_to = source.__etl_effective_to,
            target.__etl_is_active = source.__etl_is_active,
            target.__etl_is_deleted = source.__etl_is_deleted
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