# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.tvtwt.yaml"
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", True)}'

# COMMAND ----------

dist_chan_cte = spark.sql(f'''
    SELECT 
        d.distribution_channel,
        d.distribution_channel_description,
        d.__etl_keys_fprint,
        xxhash64(
            d.distribution_channel_description
        ) AS __etl_row_fprint,
        d.__etl_effective_from,
        d.__etl_effective_to,
        d.__etl_is_active,
        d.__etl_is_deleted
    FROM {env_vars.silver_catalog}.ca_cross_application_components_staging.distribution_channel_texts__casted AS d
    WHERE language_key = "E";                          
''').createOrReplaceTempView('dist_chan_cte')

# COMMAND ----------

merge_result = spark.sql(f'''
    MERGE INTO {dest_tablename} AS target
    USING dist_chan_cte AS source
        ON target.__etl_keys_fprint = source.__etl_keys_fprint
    WHEN MATCHED THEN
        UPDATE SET 
            target.distribution_channel = source.distribution_channel,
            target.distribution_channel_description = source.distribution_channel_description,
            target.__etl_row_fprint = source.__etl_row_fprint,
            target.__etl_effective_from = source.__etl_effective_from,
            target.__etl_effective_to = source.__etl_effective_to,
            target.__etl_is_active = source.__etl_is_active,
            target.__etl_is_deleted = source.__etl_is_deleted
    WHEN NOT MATCHED THEN
        INSERT (
            distribution_channel,
            distribution_channel_description,
            __etl_keys_fprint,
            __etl_row_fprint,
            __etl_effective_from,
            __etl_effective_to,
            __etl_is_active,
            __etl_is_deleted
        ) VALUES (
            source.distribution_channel,
            source.distribution_channel_description,
            source.__etl_keys_fprint,
            source.__etl_row_fprint,
            source.__etl_effective_from,
            source.__etl_effective_to,
            source.__etl_is_active,
            source.__etl_is_deleted
        );
''')

# COMMAND ----------

logger.log.info(f'Merge: {dest_tablename} {merge_result.toPandas().head(1)}')