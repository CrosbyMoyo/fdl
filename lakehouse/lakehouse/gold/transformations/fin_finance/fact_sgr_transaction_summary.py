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
    defaultValue='gold.fact.sgr_transaction_summary.yaml',
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

old_new_report_sum = spark.sql(f'''
    SELECT
        '10Q' AS datasource
        ,nr.company_code
        ,nr.month_end_date AS date_key 
        ,nr.controlling_area
        ,nr.profit_center
        ,nr.company_code
        ,nr.chart_of_accounts
        ,nr.gl_account
        ,nr.financial_statement_item
        ,nr.consolidation_unit
        ,nr.consolidation_group
        ,'' AS consolidation_reporting_item
        ,nr.consolidation_version
        ,nr.consolidation_document_type
        ,nr.segment
        ,nr.period_mode
        ,nr.posting_level
        ,nr.group_currency
        ,nr.local_currency
        ,nr.fiscal_year
        ,nr.fiscal_period
        ,sum(nr.amount_in_group_currency) AS amount_group_currency
        ,sum(nr.amount_in_local_currency) AS amount_local_currency
    FROM 
        {metadata.alias2src('nr')} AS nr
    GROUP BY 
        ALL 
''')

old_new_report_sum.createOrReplaceTempView('old_new_report_sum')

# COMMAND ----------

with_reporting_rules_sum = spark.sql(f'''
  SELECT
    '30Q' AS datasource
    ,wr.company_code 
    ,wr.month_end_date AS date_key 
    ,wr.controlling_area
    ,wr.profit_center
    ,wr.company_code
    ,wr.chart_of_accounts
    ,wr.gl_account
    ,wr.financial_statement_item
    ,wr.consolidation_unit
    ,wr.consolidation_group
    ,wr.consolidation_reporting_item
    ,wr.consolidation_version
    ,wr.consolidation_document_type
    ,wr.segment
    ,wr.period_mode
    ,wr.posting_level
    ,wr.group_currency
    ,wr.local_currency
    ,wr.fiscal_year
    ,wr.fiscal_period
    ,sum(wr.amount_in_group_currency) AS amount_group_currency
    ,sum(wr.amount_in_local_currency) AS amount_local_currency
  FROM 
    {metadata.alias2src('wr')} AS wr
  GROUP BY 
    ALL 
''')

with_reporting_rules_sum.createOrReplaceTempView('with_reporting_rules_sum')

# COMMAND ----------

sgr_transactions_combined = spark.sql(f'''
        SELECT 
            *
        FROM 
            old_new_report_sum
    UNION ALL
        SELECT 
            *
        FROM 
            with_reporting_rules_sum
''')

sgr_transactions_combined.createOrReplaceTempView('sgr_transactions_combined')

# COMMAND ----------

spark.sql(f'''
    SELECT
        f.datasource
        -- Foreign Keys
        ,{metadata.get_fkey_ddl(["f.company_code"])} AS company_code_skey
        ,f.date_key 
        ,{metadata.get_fkey_ddl(["f.profit_center", "f.controlling_area"])} AS profit_center_skey
        ,{metadata.get_fkey_ddl(["f.company_code"])} AS company_code_skey
        ,{metadata.get_fkey_ddl(["f.chart_of_accounts", "f.gl_account"])}  AS gl_account_skey
        ,{metadata.get_fkey_ddl(["f.financial_statement_item"])}  AS financial_statement_item_skey
        ,{metadata.get_fkey_ddl(["f.consolidation_unit", "f.consolidation_group"])}  AS consolidation_unit_skey
        ,{metadata.get_fkey_ddl(["f.consolidation_reporting_item"])}  AS consolidation_reporting_item_skey
        ,{metadata.get_fkey_ddl(["f.segment"])}  AS consolidation_segment_skey
        ,{metadata.get_fkey_ddl(["f.posting_level"])} AS posting_level_skey

        ,f.group_currency AS group_currency_skey
        ,f.local_currency AS local_currency_skey

        -- Payload
        ,f.period_mode
        ,f.consolidation_document_type
        ,f.fiscal_year
        ,f.fiscal_period
        -- Should this have a dimension? 
        ,f.consolidation_version 
        ,f.amount_local_currency
        ,f.amount_group_currency
    FROM 
        sgr_transactions_combined AS f
''').createOrReplaceTempView('gold_table')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_table

# COMMAND ----------

etl_fields = spark.sql(f'''
    {metadata.get_etl_fields_ddl('gold_table')}
''')
etl_fields.createOrReplaceTempView('etl_fields')

# COMMAND ----------

metadata.insert_overwrite('etl_fields', destination)