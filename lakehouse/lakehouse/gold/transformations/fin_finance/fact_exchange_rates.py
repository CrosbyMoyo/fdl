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
    defaultValue='gold.fact.fact_exchange_rates.yaml',
    label='1 - metadata_filename'
)

# COMMAND ----------

metadata_filename = dbutils.widgets.get('metadata_filename')
assert metadata_filename, 'metadata_filename must be provided'

logger.log.info(f'Widget: metadata_filename = "{metadata_filename}"')

# COMMAND ----------

metadata = GoldMetadataYaml(
    file_path=f'./metadata/{metadata_filename}',
    slv_catalog=env_vars.silver_catalog,
    gld_catalog=env_vars.gold_catalog
)

# COMMAND ----------

destination = metadata.dest_3partname(True)

# COMMAND ----------

gold_table = spark.sql(f'''
    SELECT
        -- Foreign Keys 
        f.valid_from as valid_from_key
        
        -- Keys 
        ,f.exchange_rate_type
        ,f.from_currency
        ,f.to_currency

        -- Payload 
        ,f.scaled_exchange_rate
    FROM
        {env_vars.silver_catalog}.fin_finance.exchange_rates AS f
''')

gold_table.createOrReplaceTempView('gold_table')

# COMMAND ----------

hashed_gold_table = spark.sql(f'''{metadata.get_etl_fields_ddl('gold_table')}''')

hashed_gold_table.createOrReplaceTempView('hashed_gold_table')

# COMMAND ----------

metadata.insert_overwrite('hashed_gold_table', destination)