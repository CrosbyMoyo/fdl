# Databricks notebook source
# MAGIC %md
# MAGIC ### Tabular Generic Casted Notebook
# MAGIC This is a generic notebook that processes a bronze table to the first hop of the silver layer. It will work for all tabular datasets.
# MAGIC
# MAGIC
# MAGIC **Parameters**
# MAGIC - metadata_filepath: Path to the associated YAML file note this will need to be relative for example `_./finance/metadata/silver.tcurr.yaml`

# COMMAND ----------

# MAGIC %run ../../common/properties

# COMMAND ----------

# MAGIC %run ../../common/silver_metadata

# COMMAND ----------

from pyspark.sql.functions import col, coalesce, lit, xxhash64, concat, current_date, expr
from pyspark.sql.types import DateType

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

dbutils.widgets.dropdown(
    name = 'metadata_schema',
    defaultValue = 'ca_cross_application_components',
    choices = [
        'ca_cross_application_components',
        'fin_controlling',
        'fin_finance',
        'fin_general_ledger',
        'sl_extended_warehouse_management'
        # TODO: add more here as we need them
    ],
    label = '1 - metadata_schema'
)

dbutils.widgets.text(
    name = 'metadata_filename',
    defaultValue = '',
    label = '2 - metadata_filename'
)

dbutils.widgets.text(
    name = 'stage',
    defaultValue = '',
    label = '3 - stage'
)

dbutils.widgets.dropdown(
    name = 'table_operation',
    defaultValue = 'create_or_replace',
    choices = ['create_if_not_exists', 'create_or_replace', 'merge'],
    label = '4 - table_operation'
)

dbutils.widgets.dropdown(
    name = 'test_uniqueness',
    defaultValue = 'False',
    choices = ['True', 'False'],
    label = '5 - test_uniqueness'
)

dbutils.widgets.dropdown(
    name = 'remove_duplicates',
    defaultValue = 'True',
    choices = ['True', 'False'],
    label = '6 - remove_duplicates'
)

# COMMAND ----------

metadata_schema = dbutils.widgets.get('metadata_schema')
logger.log.info(f'Widget: metadata_schema = "{metadata_schema}"')

metadata_filename = dbutils.widgets.get('metadata_filename')
logger.log.info(f'Widget: metadata_filename = "{metadata_filename}"')

stage = dbutils.widgets.get('stage')
logger.log.info(f'Widget: stage = "{stage}"')

table_operation = dbutils.widgets.get('table_operation')
logger.log.info(f'Widget: table_operation = "{table_operation}"')

run_test_uniqueness = dbutils.widgets.get('test_uniqueness')
logger.log.info(f'Widget: test_uniqueness = "{run_test_uniqueness}"')

remove_duplicates = dbutils.widgets.get('remove_duplicates')
logger.log.info(f'Widget: remove_duplicates = "{remove_duplicates}"')

# COMMAND ----------

metadata_filepath = f'./{metadata_schema}/metadata/{metadata_filename}'
logger.log.info(f'metadata_filepath = "{metadata_filepath}"')

metadata = MetadataYaml(metadata_filepath)

# COMMAND ----------

# use the metadata to read the bronze table
full_tablename = f'{env_vars.bronze_catalog}.{metadata.source_2partname()}'
logger.log.info(f'full_tablename = {full_tablename} sample = {metadata.yaml.get("source").get("sample")}')

# COMMAND ----------

# MAGIC %md
# MAGIC Read the Bronze table

# COMMAND ----------

if 'sample' in metadata.yaml.get('source'):
    bronze_table = (spark.read
        .table(full_tablename)
        .sample(**metadata.yaml.get('source').get('sample'))
    )
else:
    bronze_table = spark.read.table(full_tablename)

# COMMAND ----------

filter_obj = metadata.yaml.get('source').get('bronze_filters')
if filter_obj:
    for f in filter_obj:
        bronze_table = (
            bronze_table
            .filter(f['sql_where'])
        )

# COMMAND ----------

# MAGIC %md
# MAGIC Bulk rename columns

# COMMAND ----------

renamed_cols = [
    col(cr['name']).alias(cr['rename_to'])
    for cr in metadata.yaml['column_transformations']
]

# COMMAND ----------

logger.log.info(f'Reading and renaming {len(renamed_cols)} columns')
bronze_renamed = (
    bronze_table
    .select(*renamed_cols)
    .distinct() if remove_duplicates == 'True' else bronze_table.select(*renamed_cols)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Bulk cast columns

# COMMAND ----------

cast_cols = [
    expr(f"{env_vars.silver_catalog}.{cr['complex_cast_function']} AS {cr['rename_to']}")
    if cr.get('complex_cast_function')
    else col(cr['rename_to']).cast(cr['cast_to'])
    for cr in metadata.yaml['column_transformations']
]

# COMMAND ----------

logger.log.info(f'Casting {len(cast_cols)} columns')
bronze_casted = (
    bronze_renamed
    .select(*cast_cols)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Bulk coalesce columns
# MAGIC
# MAGIC NB: this has to be done _before_ calculating the hash values because xxhash64 does not work with `NULL` values
# MAGIC
# MAGIC GOTCHA: make sure numeric columns in the YAML file have a surrogate_null of 0 not ''!  
# MAGIC Otherwise Spark will cast them to strings.

# COMMAND ----------

logger.log.info('Coalescing NULLs')

bronze_coalesced = bronze_casted

for ct in metadata.yaml['column_transformations']:
  bronze_coalesced = (
    bronze_coalesced
    .withColumn(ct['rename_to'], coalesce(ct['rename_to'], lit(ct['surrogate_null'])))
  )

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate the fingerprint for the PKs

# COMMAND ----------

key_cols = [c['rename_to'] for c in metadata.yaml['column_transformations'] if c['column_role'] == 'PK']
logger.log.info(f'Key columns: "{key_cols}"')

# COMMAND ----------

logger.log.info('Calculating the metadata etl fields')

bronze_etl = (
    bronze_coalesced
    .withColumn('__etl_keys_fprint', xxhash64(concat(*key_cols)))
    .withColumn('__etl_effective_from', current_date())
    .withColumn('__etl_effective_to', lit(None).cast(DateType()))
    .withColumn('__etl_is_active', lit(True))
    .withColumn('__etl_is_deleted', lit(False))
)

bronze_etl.createOrReplaceTempView('bronze_etl')

# COMMAND ----------

# MAGIC %md
# MAGIC Write to the staging table

# COMMAND ----------

staging_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname(stage, True)}'
logger.log.info(f'staging_tablename = {staging_tablename}')

# COMMAND ----------

create_sql = "CREATE OR REPLACE TABLE " if table_operation == 'create_or_replace' else "CREATE TABLE IF NOT EXISTS "

# COMMAND ----------

result = spark.sql(f'''
    {create_sql} {staging_tablename}
    AS
    SELECT *
    FROM bronze_etl
''')

# COMMAND ----------

metadata.test_uniqueness(staging_tablename, run_test_uniqueness)

# COMMAND ----------

# TODO: log the number of rows inserted
display(result)

# COMMAND ----------

# TODO: amend the above:
# if the table_operation param = "create_or_replace"
#   then c_o_r
# if the table_operation param = "create_if_not_exist" then
#   - check the information_schema to see if it does
#       - if it doesn't then run c_i_n_o
#       - if it does, then run "merge"

# COMMAND ----------

# TODO: write back to the bronze table to show those rows brought in

# COMMAND ----------

