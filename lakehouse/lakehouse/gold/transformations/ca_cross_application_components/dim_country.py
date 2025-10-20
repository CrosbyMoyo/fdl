# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

from datetime import datetime, timezone

# COMMAND ----------

dbutils.widgets.text(
    name='metadata_filename',
    defaultValue='gold.dim.country.yaml',
    label='1 - metadata_filename'
)

# COMMAND ----------

metadata_filename = dbutils.widgets.get('metadata_filename')
logger.log.info(f'Widget: metadata_filename = "{metadata_filename}"')

# COMMAND ----------

metadata = GoldMetadataYaml(
    file_path = f'./metadata/{metadata_filename}',
    slv_catalog = env_vars.silver_catalog,
    gld_catalog = env_vars.gold_catalog
)

# COMMAND ----------

load_timestamp = datetime.now(timezone.utc)

c_tablename = metadata.source_3partname(
    tablename='countries',
    include_schemaversion=True
)

cn_tablename = metadata.source_3partname(
    tablename='country_names',
    include_schemaversion=True
)

cc_tablename = metadata.source_3partname(
    tablename='country_coordinates',
    include_schemaversion=True
)

# COMMAND ----------

spark.sql(f'''
    SELECT *
    FROM {cc_tablename}
''').display()

# COMMAND ----------

metadata.dest_3partname(include_schemaversion=True)

# COMMAND ----------

# NB: because dim_country uses a business key it is effectively SCD 0

gold_table = spark.sql(f'''
    WITH country_joined AS (
        SELECT
            c.country_key
            ,cn.country_name_short
            ,cn.country_name_full
            ,cc.longitude
            ,cc.latitude
        FROM
            {c_tablename} AS c
            LEFT JOIN {cn_tablename} AS cn
                ON c.country_key = cn.country_key
            LEFT JOIN {cc_tablename} AS cc
                ON c.country_key = cc.country_key
    ),
    country_metadata AS (
        SELECT
            cj.*
            ,xxhash64(
                cj.country_key
            ) AS __etl_keys_fprint
            ,xxhash64(
                cj.* EXCEPT (cj.country_key)
            ) AS __etl_row_fprint
            ,'{load_timestamp}' AS __etl_effective_from
            ,NULL AS __etl_effective_to
            ,TRUE AS __etl_is_active
            ,FALSE AS __etl_is_deleted
        FROM
            country_joined AS cj
    )
    INSERT OVERWRITE TABLE {metadata.dest_3partname(include_schemaversion=True)}
    SELECT *
    FROM country_metadata;
''')


# COMMAND ----------

display(gold_table)