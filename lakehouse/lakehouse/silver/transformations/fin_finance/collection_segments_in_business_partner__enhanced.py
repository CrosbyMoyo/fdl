# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.udmbpsegments.yaml"
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

source_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("casted", include_schemaversion=True)}'
dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", include_schemaversion=True)}'

# COMMAND ----------

spark.sql(
    f'''
        SELECT 
            {env_vars.silver_catalog}.ca_cross_application_components.extract_sap_date(src.closed_on)   AS closed_on
            ,{env_vars.silver_catalog}.ca_cross_application_components.extract_sap_date(src.created_on) AS created_on
            ,src.* EXCEPT(src.closed_on, src.created_on)
        FROM
            {source_tablename} AS src
    '''
).createOrReplaceTempView('enhanced')

# COMMAND ----------

spark.sql(
    f'''
        SELECT 
            {metadata.get_key_columns_ddl()}
            ,{metadata.get_payload_columns_ddl()}
            ,xxhash64({metadata.get_key_columns_ddl()}) AS __etl_keys_fprint
            ,xxhash64({metadata.get_payload_columns_ddl()}) AS __etl_row_fprint
            ,__etl_effective_from
            ,__etl_effective_to
            ,__etl_is_active
            ,__etl_is_deleted
        FROM
            enhanced
    '''
).createOrReplaceTempView('final')

# COMMAND ----------

merge_statement = metadata.get_merge_ddl('final', dest_tablename)
merge_result = spark.sql(merge_statement)
logger.log.info(f'Merge: {dest_tablename} {merge_result.toPandas().head(1)}')