# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata
# MAGIC

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.sgr_transactions_old_new_report_logic.yaml"
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

sgr_transactions_old_new_report_logic__casted_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("sgr_transactions_old_new_report_logic__casted")}'
dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver")}'

# COMMAND ----------

sgr_transactions_old_new_report_logic = spark.sql(f'''
          SELECT 
            s.client
            ,REGEXP_REPLACE(s.consolidation_unit, '^IC', '') AS company_code
            ,s.financial_statement_item
            ,s.consolidation_unit
            ,s.profit_center
            ,s.segment
            ,s.group_currency
            ,s.consolidation_group
            ,s.consolidation_document_type
            ,s.posting_level
            ,s.consolidation_version
            ,s.local_currency
            ,s.amount_in_local_currency
            ,s.amount_in_group_currency
            ,s.consolidation_document_number
            ,s.consolidation_posting_item
            ,s.fiscal_year
            ,s.fiscal_period
            ,s.fiscal_year_period
            ,CASE 
                WHEN s.fiscal_period = '0'
                    THEN LAST_DAY(DATE_ADD(MAKE_DATE(s.fiscal_year -1, 12, 1), 0))
                WHEN s.fiscal_period = '13'
                    THEN LAST_DAY(DATE_ADD(MAKE_DATE(s.fiscal_year, 12, 1), 0))
                ELSE LAST_DAY(MAKE_DATE(s.fiscal_year, s.fiscal_period,1))
            END AS month_end_date
            ,s.creation_date
            ,s.chart_of_accounts
            ,s.gl_account
            ,s.period_mode
            ,s.consolidation_chart_of_accounts
            ,s.controlling_area
            ,s.cost_center
            ,s.consolidation_ledger
          FROM
            {sgr_transactions_old_new_report_logic__casted_tablename} s
          ''')
sgr_transactions_old_new_report_logic.createOrReplaceTempView('enhanced')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from enhanced 

# COMMAND ----------

write_result = metadata.process_transformation_table('enhanced', env_vars.silver_catalog)
logger.log.info(f'Write: {dest_tablename} {write_result}')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vivid_bmoyo_slv.fin_finance.sgr_transactions_old_new_report_logic