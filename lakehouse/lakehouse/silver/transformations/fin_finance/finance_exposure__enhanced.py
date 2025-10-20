# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata
# MAGIC

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.finance_exposure.yaml"
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

source_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("casted", include_schemaversion=True)}'
dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", include_schemaversion=True)}'

# COMMAND ----------

spark.sql(f'''
          SELECT 
            s.company_code
            ,s.snapshot_date
            ,s.document_currency
            ,CASE WHEN s.source_system = 'S4' THEN 'S4HANA' ELSE s.source_system END AS source_system
            ,s.goods_received_not_invoiced_in_group_currency
            ,s.accounts_payable_in_group_currency
            ,s.accounts_receivable_group_currency
            ,s.general_ledger_borrowings_group_currency
            ,s.imports
            ,s.exports
            ,s.reporting_currency
            ,s.cash_balance
            ,s.goods_received_not_invoiced_in_local_currency
            ,s.accounts_receivable_in_local_currency
            ,s.imports_in_local_currency
            ,s.exports_in_local_currency
            ,s.accounts_payable_in_local_currency
            ,s.overdraft_balance
            ,s.reporting_week
            ,s.dividends_payable
            ,s.product_price_not_delivered
            ,s.__etl_effective_from
            ,s.__etl_effective_to
            ,s.__etl_is_active
            ,s.__etl_is_deleted

          FROM
            {source_tablename} s
          ''').createOrReplaceTempView('exposure')

# COMMAND ----------

spark.sql(
    f'''
        SELECT 
            {metadata.get_key_columns_ddl()},
            {metadata.get_payload_columns_ddl()}
            ,xxhash64({metadata.get_key_columns_ddl()})     AS __etl_keys_fprint
            ,xxhash64({metadata.get_key_columns_ddl()},{metadata.get_payload_columns_ddl()}) AS __etl_row_fprint
            ,src.__etl_effective_from
            ,src.__etl_effective_to
            ,src.__etl_is_active
            ,src.__etl_is_deleted
        FROM
            exposure AS src
    '''
).createOrReplaceTempView('final')

# COMMAND ----------

merge_statement = metadata.get_merge_ddl('final', dest_tablename)
merge_result = spark.sql(merge_statement)
logger.log.info(f'Merge: {dest_tablename} {merge_result.toPandas().head(1)}')