# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.tcurx.yaml"
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

source_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("casted", include_schemaversion=True)}'

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", include_schemaversion=True)}'

# COMMAND ----------

merge_result = spark.sql(f'''

    WITH enhanced AS (
        SELECT

            -- keys
            dpic.currency_key

            -- payload
            ,dpic.currency_decimal_places

            -- metadata
            ,dpic.__etl_keys_fprint
            ,dpic.__etl_effective_from
            ,dpic.__etl_effective_to
            ,dpic.__etl_is_active
            ,dpic.__etl_is_deleted
        FROM
        {source_tablename} AS dpic
    ),
    hashed AS (
        SELECT
            *
            ,xxhash64({metadata.get_payload_columns_ddl('enhanced.')}) AS __etl_row_fprint
        FROM
            enhanced
    )
    {metadata.get_merge_ddl('hashed', dest_tablename)}

''')


# COMMAND ----------

logger.log.info(f'Merge: {dest_tablename} {merge_result.toPandas().head(1)}')