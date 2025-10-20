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

spark.sql(
    f'''
        SELECT
            -- Foreign Keys 
            {metadata.get_fkey_ddl(['f.company_code'])} AS company_code_skey
            ,f.fiscal_year

            -- PAYLOAD
            ,f.posting_period
            ,f.exchange_rate
            ,f.offset_value
            ,f.payments
            ,f.amount_gbr_usd
            ,f.amount_local_currency
            ,f.opening_balance

        FROM 
            {metadata.alias2src('cc')} AS f
    '''
).createOrReplaceTempView('gold_table')

# COMMAND ----------

hashed_gold_table = spark.sql(f'''{metadata.get_etl_fields_ddl('gold_table')}''')

hashed_gold_table.createOrReplaceTempView('hashed_gold_table')

# COMMAND ----------

spark.sql(
    f'''
        INSERT OVERWRITE {metadata.dest_3partname(True)}
        (
            company_code_skey
            ,fiscal_year

            -- measures
            ,posting_period
            ,exchange_rate
            ,offset_value
            ,payments
            ,amount_gbr_usd
            ,amount_local_currency
            ,opening_balance

            -- metadata
            ,__etl_fprint
            ,__etl_load_timestamp
            ,__etl_is_active
            ,__etl_is_deleted
        )
        SELECT          
            -- PAYLOAD
            g.company_code_skey
            ,g.fiscal_year

            -- measures
            ,g.posting_period
            ,g.exchange_rate
            ,g.offset_value
            ,g.payments
            ,g.amount_gbr_usd
            ,g.amount_local_currency
            ,g.opening_balance

            --metadata
            ,0
            ,g.__etl_load_timestamp
            ,g.__etl_is_active
            ,g.__etl_is_deleted

        FROM
            gold_table g
    '''
)