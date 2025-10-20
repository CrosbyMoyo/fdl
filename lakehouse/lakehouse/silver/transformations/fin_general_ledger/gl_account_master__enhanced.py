# Databricks notebook source
# MAGIC %md
# MAGIC ## SKA1 Bronze to Silver
# MAGIC
# MAGIC Generic Notebook moves the `{bronze}.fivetran_s4p.ska1` data to a staging table defined in the metadata.
# MAGIC
# MAGIC This notebook further refines that data, and adds the payload fingerprint.  Then merges the data into `{silver}.fin_controlling.gl_account_master` table.

# COMMAND ----------

# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.ska1.yaml"
logger.log.info(f'"metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

source_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("casted", include_schemaversion=True)}'

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", include_schemaversion=True)}'

# COMMAND ----------

# get the columns to xxhash64
row_fprint_ddl = metadata.get_payload_columns_ddl("src.")
# get the columns to MATCH
match_cols_ddl = metadata.get_update_set_ddl("src.", "tgt.")
# get the target columns to INSERT
insert_cols_tgt_ddl = metadata.get_insert_ddl()
# get the source columns to INSERT
insert_cols_src_ddl = metadata.get_insert_ddl("src.")

# COMMAND ----------

gl = spark.sql(f'''
    SELECT
        src.gl_account,

        -- payload
        gld.gl_account_description,
        src.functional_area,
        src.group_account_number,
        src.balance_sheet_account_flag,
        src.gl_account_type,
        src.gl_account_subtype,
        src.client,
        src.chart_of_accounts,
        src.last_changed_timestamp,
        CASE
        WHEN src.gl_account in (
            '0550200010',
            '0620200010',
            '0620700090',
            '0620700270',
            '0620720020'
        ) THEN TRUE
        ELSE FALSE
        END AS government_receivable_flag,

        -- metadata
        src.__etl_keys_fprint,
        xxhash64({row_fprint_ddl}) AS __etl_row_fprint,
        src.__etl_effective_from,
        src.__etl_effective_to,
        src.__etl_is_active,
        src.__etl_is_deleted
    FROM
        {source_tablename} AS src
    LEFT JOIN {env_vars.silver_catalog}.fin_general_ledger.gl_account_descriptions AS gld
        ON 
            src.gl_account = gld.gl_account 
                AND 
            src.chart_of_accounts = gld.chart_of_accounts
                AND
            gld.language_key = "E";
''').createOrReplaceTempView("gl")

# COMMAND ----------

merge_result = spark.sql(f'''
    MERGE INTO {dest_tablename} AS tgt
    USING gl AS src
        ON tgt.__etl_keys_fprint = src.__etl_keys_fprint
    WHEN MATCHED THEN
    UPDATE
    SET
    { match_cols_ddl },
    tgt.gl_account_description = src.gl_account_description,
    tgt.government_receivable_flag = src.government_receivable_flag,
    tgt.__etl_row_fprint = src.__etl_row_fprint,
    tgt.__etl_effective_from = src.__etl_effective_from,
    tgt.__etl_effective_to = src.__etl_effective_to,
    tgt.__etl_is_active = src.__etl_is_active,
    tgt.__etl_is_deleted = src.__etl_is_deleted
    WHEN NOT MATCHED THEN
    INSERT
    (
        { insert_cols_tgt_ddl },
        government_receivable_flag,
        gl_account_description
    )
    VALUES
    (
        { insert_cols_src_ddl },
        src.government_receivable_flag,
        src.gl_account_description
    );
''')

# COMMAND ----------

logger.log.info(f'Merge: {dest_tablename} {merge_result.toPandas().head(1)}')