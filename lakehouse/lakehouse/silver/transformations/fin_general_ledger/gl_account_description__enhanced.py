# Databricks notebook source
# MAGIC %md
# MAGIC ## SKAT Bronze to Silver
# MAGIC
# MAGIC Generic Notebook moves the `{bronze}.fivetran_s4p.skat` data to a staging table defined in the metadata.
# MAGIC
# MAGIC This notebook further refines that data, and adds the payload fingerprint.  Then merges the data into `{silver}.fin_controlling.gl_account_descriptions` table.

# COMMAND ----------

# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

# change the yaml file destination
metadata_filename = "silver.skat.yaml"
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

casted_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("casted", True)}'
dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", True)}'

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

gld = spark.sql(f'''
    SELECT 
        src.client,
        src.language_key,
        src.gl_account,
        src.chart_of_accounts,
        src.gl_account_description,
        src.__etl_keys_fprint,
        xxhash64({row_fprint_ddl}) AS __etl_row_fprint,
        src.__etl_effective_from,
        src.__etl_effective_to,
        src.__etl_is_active,
        src.__etl_is_deleted
    FROM {casted_tablename} AS src;                
''').createOrReplaceTempView("gld")

# COMMAND ----------

merge_result = spark.sql(f'''    
    MERGE INTO {dest_tablename} AS tgt
    USING gld AS src
        ON tgt.__etl_keys_fprint = src.__etl_keys_fprint
    WHEN MATCHED THEN
        UPDATE SET 
            {match_cols_ddl},
            tgt.__etl_row_fprint = src.__etl_row_fprint,
            tgt.__etl_effective_from = src.__etl_effective_from,
            tgt.__etl_effective_to = src.__etl_effective_to,
            tgt.__etl_is_active = src.__etl_is_active,
            tgt.__etl_is_deleted = src.__etl_is_deleted
    WHEN NOT MATCHED THEN
        INSERT (
            {insert_cols_tgt_ddl}
        ) VALUES (
            {insert_cols_src_ddl}
        );
''')

# COMMAND ----------

logger.log.info(f'Merge: {dest_tablename} {merge_result.toPandas().head(1)}')