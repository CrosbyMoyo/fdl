# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.consolidation_reporting_rule_variant.yaml"
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

reporting_rule_id_table_name = f'{env_vars.silver_catalog}.{metadata.sources_2partname("reporting_rule_id__casted", True)}'
reporting_rule_id_descriptions_table_name = f'{env_vars.silver_catalog}.{metadata.sources_2partname("reporting_rule_id_descriptions__casted", True)}'
reporting_rule_variant_assignment_to_coa_and_hierarchy_table_name = f'{env_vars.silver_catalog}.{metadata.sources_2partname("reporting_rule_variant_assignment_to_coa_and_hierarchy__casted", True)}'

# COMMAND ----------

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", include_schemaversion=True)}'

# COMMAND ----------

consolidation_reporting_item = spark.sql(
    f"""
        SELECT
             rri.client
            ,rri.reporting_rule_variant
            ,rrva.reporting_item_hierarchy
            ,rrva.consolidation_chart_of_accounts
            ,rrid.description
        FROM
            {reporting_rule_id_table_name} AS rri
        LEFT JOIN
            {reporting_rule_id_descriptions_table_name} AS rrid
            ON rri.reporting_rule_variant = rrid.reporting_rule_variant
        LEFT JOIN
            {reporting_rule_variant_assignment_to_coa_and_hierarchy_table_name} AS rrva
            ON rri.reporting_rule_variant = rrva.reporting_rule_variant
    """
)

consolidation_reporting_item.createOrReplaceTempView('enhanced')

# COMMAND ----------

write_result = metadata.process_transformation_table('enhanced', env_vars.silver_catalog)
logger.log.info(f'Write: {dest_tablename} {write_result}')