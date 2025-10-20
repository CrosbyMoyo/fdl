# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

from datetime import datetime, timezone

load_timestamp = datetime.now(timezone.utc)

# COMMAND ----------

dbutils.widgets.text(
    name='metadata_filename',
    defaultValue='',
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

destination = metadata.dest_3partname(True)

# COMMAND ----------

spark.sql(
    f'''
      WITH oil_prices 
        AS(
            SELECT
              TO_CHAR(f.date_key, 'yyyy-MM') AS year_month
              ,f.symbol
              ,avg(f.price_local_currency) AS avg_price
            FROM
                 {metadata.alias2src('op')} AS f
            WHERE 
                symbol IN ('AAWZA00', 'AAVJI00', 'IFFB')
            GROUP BY 
                TO_CHAR(f.date_key, 'yyyy-MM')
                ,f.symbol
        ),
            base_oil_prices 
        AS
          (
            SELECT
              TO_CHAR(f.date_key, 'yyyy-MM') AS year_month
              ,f.symbol
              ,f.avg_price
            FROM
                 {metadata.alias2src('bc')} AS f
            WHERE
                symbol = 'BASEOIL'
        )
        SELECT
          op.year_month
          ,op.symbol
          ,op.avg_price
        FROM 
          oil_prices op

        UNION

        SELECT
          bop.year_month
          ,bop.symbol
          ,bop.avg_price
        FROM 
          base_oil_prices bop
''').createOrReplaceTempView('oil_prices_agg')

# COMMAND ----------

hashed_gold_table = spark.sql(f'''{metadata.get_etl_fields_ddl('oil_prices_agg')}''')

hashed_gold_table.createOrReplaceTempView('hashed_gold_table')

# COMMAND ----------

metadata.insert_overwrite('hashed_gold_table', destination)