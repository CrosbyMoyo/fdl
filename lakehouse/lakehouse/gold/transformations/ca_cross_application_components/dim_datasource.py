# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

dbutils.widgets.text(
    name='metadata_filename',
    defaultValue='gold.dim.datasource.yaml',
    label='1 - metadata_filename'
)

# COMMAND ----------

metadata_filename = dbutils.widgets.get('metadata_filename')
logger.log.info(f'Widget: metadata_filename = "{metadata_filename}"')

# COMMAND ----------

metadata = GoldMetadataYaml(
    file_path=f'./metadata/{metadata_filename}',
    slv_catalog=env_vars.silver_catalog,
    gld_catalog=env_vars.gold_catalog
)


# COMMAND ----------

dest_tablename = metadata.dest_3partname(
    include_schemaversion=True
)

# COMMAND ----------

# TODO: Should this be a seed file or something?

# COMMAND ----------

gold_table_query = f'''
    SELECT *
    FROM
        VALUES
            ("S4HANA", "SAP S4HANA ERP system"),
            ("SAC", "SAP Analytics Cloud"),
            ("SGR", "SAP Group Reporting"),
            ("SGRJNLS", "SAP Group Reporting Manual Adjustments"),
            ("HFM", "Hyperion Financial Management Consolidation"),
            ("VBOX", "Uploaded through VBox file sharing app")
        AS (
            datasource_name,
            datasource_description
        )
'''

# COMMAND ----------

write_result = metadata.process_dim_transformation_query(gold_table_query)
logger.log.info(f'Write: {dest_tablename} {write_result}')