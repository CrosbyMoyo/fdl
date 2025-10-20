# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

dbutils.widgets.text(
    name='metadata_filename',
    defaultValue='gold.fact.exposure.yaml',
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

gold_table = spark.sql(f'''
    SELECT
        f.snapshot_date AS snapshot_date_key,
        {metadata.get_fkey_ddl(['f.company_code'])} AS company_code_skey,
        {metadata.get_fkey_ddl(['f.source_system'])} AS datasource_skey,
        f.goods_received_not_invoiced_in_group_currency,
        f.accounts_payable_in_group_currency,
        f.accounts_receivable_group_currency,
        f.general_ledger_borrowings_group_currency,
        f.imports,
        f.exports,
        f.reporting_currency,
        f.cash_balance,
        f.goods_received_not_invoiced_in_local_currency,
        f.accounts_receivable_in_local_currency,
        f.imports_in_local_currency,
        f.exports_in_local_currency,
        f.accounts_payable_in_local_currency,
        f.overdraft_balance,
        f.document_currency,
        f.dividends_payable,
        f.product_price_not_delivered
    FROM {env_vars.silver_catalog}.fin_finance.finance_exposure AS f        
''')

gold_table.createOrReplaceTempView('gold_table')

# COMMAND ----------

etl_fields = spark.sql(f'''
    {metadata.get_etl_fields_ddl('gold_table')}
''')
etl_fields.createOrReplaceTempView('etl_fields')

# COMMAND ----------

metadata.insert_overwrite('etl_fields', destination)