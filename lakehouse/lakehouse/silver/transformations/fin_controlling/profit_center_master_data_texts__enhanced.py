# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.cepct.yaml"
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

source_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("casted", True)}'
dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", True)}'

# COMMAND ----------

prctr = spark.sql(f'''
    SELECT 
        p.language_key,
        p.profit_center,
        {env_vars.silver_catalog}.ca_cross_application_components.extract_sap_date(p.valid_to) AS valid_to,
        p.controlling_area,
        p.profit_center_description_long AS profit_center_description, 
        p.__etl_keys_fprint,
        xxhash64(
            profit_center_description
        ) AS __etl_row_fprint,
        p.__etl_effective_from,
        p.__etl_effective_to,
        p.__etl_is_active,
        p.__etl_is_deleted
    FROM {source_tablename} AS p;      
''').createOrReplaceTempView('prctr')

# COMMAND ----------

merge_result = spark.sql(f'''
    MERGE INTO {dest_tablename} AS target
    USING prctr AS source
        ON target.__etl_keys_fprint = source.__etl_keys_fprint
    WHEN MATCHED THEN
        UPDATE SET
            target.language_key = source.language_key, 
            target.profit_center = source.profit_center,
            target.valid_to = source.valid_to,
            target.controlling_area = source.controlling_area,
            target.profit_center_description = source.profit_center_description,
            target.__etl_row_fprint = source.__etl_row_fprint,
            target.__etl_effective_from = source.__etl_effective_from,
            target.__etl_effective_to = source.__etl_effective_to,
            target.__etl_is_active = source.__etl_is_active,
            target.__etl_is_deleted = source.__etl_is_deleted
    WHEN NOT MATCHED THEN
        INSERT (
            language_key,
            profit_center,
            valid_to,
            controlling_area,
            profit_center_description,
            __etl_keys_fprint,
            __etl_row_fprint,
            __etl_effective_from,
            __etl_effective_to,
            __etl_is_active,
            __etl_is_deleted
        ) VALUES (
            source.language_key,
            source.profit_center,
            source.valid_to,
            source.controlling_area,
            source.profit_center_description,
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