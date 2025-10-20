# Databricks notebook source
# MAGIC %md
# MAGIC ## CSKS Bronze to Silver
# MAGIC
# MAGIC Generic Notebook moves the `{bronze}.fivetran_s4p.cosp` data to a staging table defined in the metadata.
# MAGIC
# MAGIC This notebook further refines that data, and adds the payload fingerprint.  Then merges the data into `{silver}.fin_controlling.cosp` table.

# COMMAND ----------

# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.cosp.yaml"
logger.log.info(f'"metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

source_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("casted", include_schemaversion=True)}'

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", include_schemaversion=True)}'

# COMMAND ----------

spark.sql(f'''
            SELECT
                src.*
            FROM {source_tablename} AS src

          ''').createOrReplaceTempView("deduped")

# COMMAND ----------

spark.sql(
    f'''
        SELECT 
            {metadata.get_key_columns_ddl()},
            {metadata.get_payload_columns_ddl()}
            ,xxhash64({metadata.get_key_columns_ddl()})     AS __etl_keys_fprint
            ,xxhash64({metadata.get_payload_columns_ddl()}) AS __etl_row_fprint
            ,src.__etl_effective_from
            ,src.__etl_effective_to
            ,src.__etl_is_active
            ,src.__etl_is_deleted
        FROM
            deduped AS src
    '''
).createOrReplaceTempView('final')

# COMMAND ----------

merge_statement = metadata.get_merge_ddl('final', dest_tablename)
merge_result = spark.sql(merge_statement)
logger.log.info(f'Merge: {dest_tablename} {merge_result.toPandas().head(1)}')