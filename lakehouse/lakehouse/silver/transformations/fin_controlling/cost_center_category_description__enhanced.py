# Databricks notebook source
# MAGIC %md
# MAGIC ## TKT05 Bronze to Silver
# MAGIC
# MAGIC Generic Notebook moves the `{bronze}.fivetran_s4p.tkt05` data to a staging table defined in the metadata.
# MAGIC
# MAGIC This notebook further refines that data, and adds the payload fingerprint.  Then merges the data into `{silver}.fin_controlling.cost_center_category_description` table.

# COMMAND ----------

# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.tkt05.yaml"
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

casted_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("casted", True)}'
dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", True)}'

# COMMAND ----------

# get the columns to xxhash64
row_fprint_ddl = metadata.get_payload_columns_ddl("cd.")
# get the columns to MATCH
match_cols_ddl = metadata.get_update_set_ddl("source.", "target.")
# get the target columns to INSERT
insert_cols_tgt_ddl = metadata.get_insert_ddl()
# get the source columns to INSERT
insert_cols_src_ddl = metadata.get_insert_ddl("source.")

# COMMAND ----------

category_description = spark.sql(f'''
    SELECT 
        cd.client,
        cd.language_key,
        cd.cost_center_category,
        cd.cost_center_category_description,
        cd.__etl_keys_fprint,
        xxhash64({row_fprint_ddl}) AS __etl_row_fprint,
        cd.__etl_effective_from,
        cd.__etl_effective_to,
        cd.__etl_is_active,
        cd.__etl_is_deleted
    FROM {casted_tablename} AS cd
        WHERE cd.language_key = "E";                   
''').createOrReplaceTempView('category_description')

# COMMAND ----------

merge_result = spark.sql(f'''    
    MERGE INTO {dest_tablename} AS target
    USING category_description AS source
        ON target.__etl_keys_fprint = source.__etl_keys_fprint
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
        ) VALUES (
            {insert_cols_src_ddl}
        );
''')

# COMMAND ----------

logger.log.info(f'Merge: {dest_tablename} {merge_result.toPandas().head(1)}')