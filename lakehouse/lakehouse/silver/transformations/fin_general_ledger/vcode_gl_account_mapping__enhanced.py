# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.zrtr_vcodes.yaml"
logger.log.info(f'Widget: metadata_filename = "{metadata_filename}"')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

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

source_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("casted", include_schemaversion=True)}'

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", include_schemaversion=True)}'

# COMMAND ----------

enhanced = spark.sql(f'''
    SELECT
        -- keys
        src.client
        ,src.gl_account
        ,src.cost_center_category

        -- payload
        ,CONCAT(CONCAT(src.gl_account, '_'), src.cost_center_category) AS vcode_join_field
        ,src.vcode

        -- metadata
        ,src.__etl_keys_fprint
        ,src.__etl_effective_from
        ,src.__etl_effective_to
        ,src.__etl_is_active
        ,src.__etl_is_deleted
    FROM
    {source_tablename} AS src;           
''').createOrReplaceTempView("enhanced")

# COMMAND ----------

hashed = spark.sql(f'''
    SELECT
        *
        ,xxhash64({metadata.get_payload_columns_ddl('enhanced.')}, vcode_join_field) AS __etl_row_fprint
    FROM
        enhanced                           
''').createOrReplaceTempView("hashed")

# COMMAND ----------

# read the data from the casted table
merge_result = spark.sql(f'''
    MERGE INTO {dest_tablename} AS tgt
    USING hashed AS src
        ON tgt.__etl_keys_fprint = src.__etl_keys_fprint
    WHEN MATCHED THEN
        UPDATE SET 
            {match_cols_ddl},
            tgt.vcode_join_field = src.vcode_join_field,
            tgt.__etl_row_fprint = src.__etl_row_fprint,
            tgt.__etl_effective_from = src.__etl_effective_from,
            tgt.__etl_effective_to = src.__etl_effective_to,
            tgt.__etl_is_active = src.__etl_is_active,
            tgt.__etl_is_deleted = src.__etl_is_deleted
    WHEN NOT MATCHED THEN
        INSERT (
            {insert_cols_tgt_ddl},
            vcode_join_field
        ) VALUES (
            {insert_cols_src_ddl},
            src.vcode_join_field
        );

''')


# COMMAND ----------

logger.log.info(f'Merge: {dest_tablename} {merge_result.toPandas().head(1)}')