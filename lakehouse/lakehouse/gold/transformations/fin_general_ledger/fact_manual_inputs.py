# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

dbutils.widgets.text(
    name='metadata_filename',
    defaultValue='gold.fact.inputs.yaml',
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

manual_inputs_tablename = metadata.source_3partname(
    tablename='inputs',
    include_schemaversion=False
).replace("slv","brz")

dest_tablename = metadata.dest_3partname(
    include_schemaversion=True
)


# COMMAND ----------

spark.sql(
    f'''
        INSERT OVERWRITE TABLE {env_vars.gold_catalog}.fin_general_ledger.fact_manual_inputs
        BY NAME
        SELECT
            f.input_id AS input_id,
            f.user_email AS user_email,
            f.user_name AS user_name,
            f.country AS country,
            f.group_code AS group_code,
            f.page AS page,
            f.input_type AS input_type,
            f.period AS period,
            f.value AS value,
            f.active_flag AS active_flag,
            f.timestamp AS timestamp
        FROM
            {manual_inputs_tablename} AS f
    '''
)