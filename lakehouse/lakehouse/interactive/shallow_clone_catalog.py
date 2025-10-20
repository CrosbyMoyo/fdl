# Databricks notebook source
# MAGIC %md 
# MAGIC ## Utilities: Shallow Clone Catalog 
# MAGIC This notebook will make copy of a source catalog and all the schemata in it then shallow clone the tables. Note this does not copy the functions. This can be executed on a serverless compute.
# MAGIC
# MAGIC
# MAGIC Parameters: 
# MAGIC - source_catalog: The name of the source catalog to copy 
# MAGIC - target_catalog: The name of the new catalog that will be created containing the tables and schemata from the source catalog 
# MAGIC - default_identity: The name of an identity or group to provide all privileges on the created catalog

# COMMAND ----------

dbutils.widgets.text('source_catalog', '')
dbutils.widgets.text('target_catalog', '')
dbutils.widgets.text('default_identity', 'data-engineers')

source_catalog = dbutils.widgets.get('source_catalog')
target_catalog = dbutils.widgets.get('target_catalog')
default_identity = dbutils.widgets.get('default_identity')

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor

def execute_queries_in_parallel(queries: list, max_workers: int=15) -> None:
  """ Execute all notebooks in a workspace directory in parallel"""
  with ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = [executor.submit(spark.sql, query) for query in queries]
    for future in futures:
      try:
        future.result()
      except Exception as e:
          raise e

# COMMAND ----------

spark.sql(f'CREATE CATALOG IF NOT EXISTS {target_catalog}')
spark.sql(f'GRANT ALL PRIVILEGES ON CATALOG {target_catalog} TO `{default_identity}`')

# COMMAND ----------

schema_queries = spark.sql(
    f"""
        SELECT
            s.catalog_name
            ,s.schema_name
            ,'CREATE SCHEMA IF NOT EXISTS ' || '{target_catalog}' || '.' || s.schema_name AS create_query
            ,'GRANT ALL PRIVILEGES ON SCHEMA ' || '{target_catalog}' || '.' || s.schema_name || ' TO `{default_identity}`'  AS grant_query
        FROM 
            {source_catalog}.information_schema.schemata AS s 
        WHERE 
            s.schema_name NOT IN ('default', 'information_schema', 'hive_metastore')
    """
)

execute_queries_in_parallel([query.create_query for query in schema_queries.collect()])
execute_queries_in_parallel([query.grant_query for query in schema_queries.collect()])

# COMMAND ----------

table_queries = spark.sql(
    f"""
        SELECT
            t.table_catalog,
            t.table_schema,
            t.table_name,
            concat(
                'CREATE OR REPLACE TABLE ', '{target_catalog}', '.', t.table_schema, '.', t.table_name,
                ' SHALLOW CLONE ', '{source_catalog}',  '.', t.table_schema, '.', t.table_name
            ) AS query
        FROM 
            {source_catalog}.information_schema.tables AS t
        WHERE 
            t.table_schema NOT IN ('default', 'information_schema', 'hive_metastore', 'powerbi', 'sap_datasphere', 'sap_sac', 'capex_sql_server')
            AND t.table_type NOT IN ('VIEW')
    """
)
execute_queries_in_parallel([query.query for query in table_queries.collect()])