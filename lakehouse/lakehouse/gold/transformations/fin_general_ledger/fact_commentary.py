# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

dbutils.widgets.text(
    name='metadata_filename',
    defaultValue='gold.fact.commentary.yaml',
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

commentary_tablename = metadata.source_3partname(
    tablename='commentary',
    include_schemaversion=False
).replace("slv","brz")

dest_tablename = metadata.dest_3partname(
    include_schemaversion=True
)


# COMMAND ----------

spark.sql(
    f'''
        INSERT OVERWRITE TABLE {env_vars.gold_catalog}.fin_general_ledger.fact_commentary
        BY NAME
        SELECT
            f.comment_id AS comment_id,
            f.comment AS comment,
            f.commenter_email AS commenter_email,
            f.commenter_name AS commenter_name,
            f.region AS region,
            f.country AS country,
            f.feedback_ind AS feedback_ind,
            f.group_code AS group_code,
            f.month AS month,
            f.page AS page,
            f.report AS report,
            f.soft_delete AS soft_delete,
            f.subject AS subject,
            f.timestamp AS timestamp,
            f.year AS year
        FROM
            {commentary_tablename} AS f
    '''
)