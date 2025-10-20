# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.consolidation_segment.yaml"
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

consolidation_segment_table_name = f'{env_vars.silver_catalog}.{metadata.sources_2partname("finance_reporting_segments__casted", True)}'

# COMMAND ----------

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", include_schemaversion=True)}'

# COMMAND ----------

consolidation_unit = spark.sql(
    f"""
        SELECT
             cs.client
            ,cs.segment
            ,cs.description
        FROM
            {consolidation_segment_table_name} AS cs
    """
)

consolidation_unit.createOrReplaceTempView('enhanced')

# COMMAND ----------

write_result = metadata.process_transformation_table('enhanced', env_vars.silver_catalog)
logger.log.info(f'Write: {dest_tablename} {write_result}')