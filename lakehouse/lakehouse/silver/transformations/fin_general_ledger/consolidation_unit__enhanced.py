# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.consolidation_unit.yaml"
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

consolidation_unit_table_name = f'{env_vars.silver_catalog}.{metadata.sources_2partname("consolidation_unit__casted", True)}'
consolidation_unit__description_table_name = f'{env_vars.silver_catalog}.{metadata.sources_2partname("consolidation_unit_descriptions__casted", True)}'
consolidation_group_structure_table_name = f'{env_vars.silver_catalog}.{metadata.sources_2partname("consolidation_group_structure__casted", True)}'

# COMMAND ----------

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", include_schemaversion=True)}'

# COMMAND ----------

consolidation_unit = spark.sql(
    f"""
        SELECT
             cu.client
            ,cu.dimension
            ,cu.consolidation_unit
            ,CASE
                WHEN cud.consolidation_unit_long_description = ''
                    THEN cud.consolidation_unit_medium_description
                ELSE cud.consolidation_unit_long_description
            END AS consolidation_unit_description
            ,coalesce(cgs.consolidation_group, '') AS consolidation_group
            ,cu.country_region
            ,cu.company
        FROM
            {consolidation_unit_table_name} AS cu
        LEFT JOIN
            {consolidation_unit__description_table_name} AS cud
            ON cu.consolidation_unit = cud.consolidation_unit
            AND cud.language_key = 'E'
        LEFT JOIN
            {consolidation_group_structure_table_name} AS cgs
            ON cu.consolidation_unit = cgs.consolidation_unit
            AND cgs.fiscal_year_period_to = '9999999'
    """
)

consolidation_unit.createOrReplaceTempView('enhanced')

# COMMAND ----------

write_result = metadata.process_transformation_table('enhanced', env_vars.silver_catalog)
logger.log.info(f'Write: {dest_tablename} {write_result}')