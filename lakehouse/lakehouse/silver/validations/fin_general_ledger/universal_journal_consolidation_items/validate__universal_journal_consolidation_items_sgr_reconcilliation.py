# Databricks notebook source
# MAGIC %md
# MAGIC ## Validate: Universal Journal Consolidation Items SGR Datasphere Reconcilliation
# MAGIC #### Purpose:
# MAGIC This notebook compares the values of the `slv.fin_general_ledger.universal_journal_consolidation_items` to the `brz.sap_datasphere.2vf_fi_reporting_001` AND validates the following business logic: 
# MAGIC
# MAGIC 1. `universal_journal_consolidation_items.quantity` = 0
# MAGIC 2. `universal_journal_consolidation_items.amount_in_global_currency` = 0
# MAGIC 3. `universal_journal_consolidation_items.amount_in_company_currency` = 0
# MAGIC
# MAGIC #### Pass/Fail Criteria:
# MAGIC - **Pass Criteria:** There are no records with differences between VIVID AND Datasphere within 2 decimal places 
# MAGIC - **Fail Criteria:** There is at least 1 record with a difference within 2 decimal places 
# MAGIC
# MAGIC #### Notes:
# MAGIC - This only validates data for the period of '2024-12'
# MAGIC - This compares an aggregate based on gl_account, profit_center, cost_center, document_type, partner_unit, financial_statement_item due to the differing granularity 
# MAGIC - Only compares SGR records
# MAGIC
# MAGIC #### Results:
# MAGIC
# MAGIC - There are few records that are not fully reconciling due to currency conversion. 
# MAGIC - Some of the records coming from Manual Adjustments, SGRJNLS are not up-to-date. Causing reconciliation issues.

# COMMAND ----------

# MAGIC %run ../../../../common/properties

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

sgr_reconcilliation = spark.sql(
  f'''
    WITH datasphere AS (
      SELECT
          RBUPTR
        , DOCTY
        , Company_Code
        , Cost_Center
        , Profit_Center
        , RITEM
        , GL_Account
        , Datasource
        , sum(Reporting_QuantityL20) as ds_quantity
        , sum(Amount_LC) as ds_amount_lc
        , sum(Amount_GC) as ds_amount_gc
      from
        {env_vars.bronze_catalog}.sap_datasphere.`2vf_fi_reporting_001`
      WHERE 1=1
        AND vcode NOT LIKE '%V.%'
        AND YearPeriod = '2024012'
        AND Datasource in ( 'SGR', 'SGRJNLS')
      GROUP BY ALL
    )

    , vivid AS (
      SELECT 
          partner_unit
        , document_type
        , company_code
        , cost_center
        , profit_center
        , financial_statement_item
        , gl_account
        , datasource
        , sum(quantity) as vvd_quantity
        , sum(amount_in_local_currency) as vvd_amount_lc
        , sum(amount_in_group_currency) as vvd_amount_gc
      from
        {env_vars.silver_catalog}.fin_general_ledger.universal_journal_consolidation_items
      WHERE
          fiscal_year_period = '2024012'
      GROUP BY ALL 
    )

    , comparison AS (
      SELECT 
        datasphere.RBUPTR 
        ,datasphere.DOCTY
        ,datasphere.RITEM
        ,datasphere.Profit_Center
        ,IFNULL(datasphere.Cost_Center, '') as Cost_Center
        ,datasphere.Company_Code
        ,datasphere.GL_Account
        ,datasphere.Datasource
        ,'#'
        ,datasphere.ds_quantity
        ,vivid.vvd_quantity
        ,datasphere.ds_quantity - vivid.vvd_quantity AS var__quantity
        ,'#'
        ,datasphere.ds_amount_lc
        ,vivid.vvd_amount_lc
        ,datasphere.ds_amount_lc - vivid.vvd_amount_lc AS var__amount_in_local_currency
        ,'#'
        ,datasphere.ds_amount_gc
        ,vivid.vvd_amount_gc
        ,datasphere.ds_amount_gc - vivid.vvd_amount_gc AS var__amount_in_group_currency
        , CASE 
              WHEN abs(try_divide(var__quantity, vvd_quantity)) > 0.1 THEN true 
              WHEN vvd_quantity IS NULL THEN true 
              WHEN var__quantity > 0.1 THEN true 
              ELSE false 
          END AS err_quantity_flag

        , CASE 
              WHEN abs(try_divide(var__amount_in_local_currency, vvd_amount_lc)) > 0.1 THEN true 
              WHEN vvd_amount_lc IS NULL THEN true 
              WHEN var__amount_in_local_currency > 0.1 THEN true 
              ELSE false 
          END AS err_amount_lc_flag

        , CASE 
              WHEN abs(try_divide(var__amount_in_group_currency, vvd_amount_gc)) > 0.1 THEN true 
              WHEN vvd_amount_gc IS NULL THEN true 
              WHEN var__amount_in_group_currency > 0.1 THEN true 
              ELSE false 
          END AS err_amount_gc_flag

      FROM
        datasphere 

        LEFT JOIN vivid ON 
          datasphere.GL_Account = vivid.gl_account
          AND datasphere.Profit_Center = vivid.profit_center
          AND datasphere.Cost_Center = vivid.cost_center
          AND datasphere.Company_Code = vivid.company_code
          AND datasphere.RITEM = vivid.financial_statement_item
          AND datasphere.RBUPTR = vivid.partner_unit
          AND datasphere.Datasource = vivid.datasource
          AND datasphere.DOCTY = vivid.document_type

    )

    SELECT
      *
    FROM
      comparison
    WHERE
      (err_amount_gc_flag  = true or err_amount_lc_flag = true or err_quantity_flag = true or vvd_amount_lc is null)
        AND 
      (ds_amount_lc <> 0 and ds_amount_gc <> 0)
  '''
)

sgr_reconcilliation.display()
assert sgr_reconcilliation.count() == 0, 'Differences between datasphere AND vivid found with SGR records!'