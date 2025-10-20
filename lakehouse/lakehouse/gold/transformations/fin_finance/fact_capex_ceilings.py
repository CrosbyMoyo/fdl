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
        SELECT
            -- Foreign Keys 
            f.budget_year
            ,{metadata.get_fkey_ddl(['f.company_code'])} AS company_code_skey
            ,{metadata.get_fkey_ddl(['f.lob1_Ceiling'])} AS line_of_business_skey
            ,f.country_code AS country_skey
            ,{metadata.get_fkey_ddl(['f.subtype_name'])} AS subtype_name_skey
            ,f.currency_key AS currency_skey

            -- PAYLOAD
            ,f.ceiling
        FROM 
            {metadata.alias2src('cc')} AS f
    '''
).createOrReplaceTempView('gold_table')

# COMMAND ----------

hashed_gold_table = spark.sql(f'''
    {metadata.get_etl_fields_ddl('gold_table')}
''')
hashed_gold_table.createOrReplaceTempView('hashed_gold_table')

# COMMAND ----------

metadata.insert_overwrite('hashed_gold_table', destination)