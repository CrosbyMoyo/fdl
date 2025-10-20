# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata
# MAGIC

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.sgr_transactions_with_reporting_rules.yaml"
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

sgr_transactions_with_reporting_rules__casted_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("sgr_transactions_with_reporting_rules__casted")}'
dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", include_schemaversion=True)}'

# COMMAND ----------

sgr_transactions_with_reporting_rules = spark.sql(f'''
          SELECT 
            s.client
            ,REGEXP_REPLACE(s.consolidation_unit, '^IC', '') AS company_code
            ,s.consolidation_reporting_item
            ,s.consolidation_version
            ,s.fiscal_year_period
            ,s.period_mode
            ,s.consolidation_chart_of_accounts
            ,s.consolidation_document_number
            ,s.consolidation_posting_item
            ,s.consolidation_group
            ,s.consolidation_unit
            ,s.gl_account
            ,s.financial_statement_item
            ,s.profit_center
            ,s.segment
            ,s.fiscal_year
            ,s.fiscal_period
            ,s.group_currency
            ,s.local_currency
            ,s.amount_in_local_currency
            ,s.amount_in_group_currency
            ,s.debit_credit_code
            ,s.posting_level
            ,s.chart_of_accounts
            ,s.consolidation_document_type
            ,s.controlling_area
            ,s.creation_date
            ,CASE 
                WHEN s.fiscal_period = '0'
                    THEN LAST_DAY(DATE_ADD(MAKE_DATE(s.fiscal_year -1, 12, 1), 0))
                ELSE LAST_DAY(MAKE_DATE(s.fiscal_year, s.fiscal_period,1))
            END AS month_end_date
          FROM
            {sgr_transactions_with_reporting_rules__casted_tablename} s
          ''')
sgr_transactions_with_reporting_rules.createOrReplaceTempView('enhanced')

# COMMAND ----------

write_result = metadata.process_transformation_table('enhanced', env_vars.silver_catalog)
logger.log.info(f'Write: {dest_tablename} {write_result}')