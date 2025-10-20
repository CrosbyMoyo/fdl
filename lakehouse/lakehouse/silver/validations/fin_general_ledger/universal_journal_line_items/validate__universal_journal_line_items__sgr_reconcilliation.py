# Databricks notebook source
# MAGIC %md
# MAGIC ## Validate: Universal Journal Line Items SGR Datasphere Reconcilliation
# MAGIC #### Purpose:
# MAGIC This notebook compares the values of the `slv.fin_general_ledger.universal_line_items` to the `brz.sap_datasphere.2vf_fi_reporting_001` AND validates the following business logic: 
# MAGIC 1. `2vf_fi_reporting_001.Reporting_QuantityLPG` = `universal_line_items.vcode_amount_local` AND `vcode_amount_group` with vcode = 'V.001' 
# MAGIC 2. `2vf_fi_reporting_001.Reporting_QuantityL20` = `universal_line_items.vcode_amount_local` AND `vcode_amount_group` with vcode = 'V.002'
# MAGIC 3. `universal_line_items.quantity` = 0 for vcode = 'V.001' AND vcode = 'V.002'
# MAGIC 4. `universal_line_items.amount_in_global_currency` = 0 for vcode = 'V.001' AND vcode = 'V.002'
# MAGIC 3. `universal_line_items.amount_in_company_currency` = 0 for vcode = 'V.001' AND vcode = 'V.002'
# MAGIC
# MAGIC #### Pass/Fail Criteria:
# MAGIC - **Pass Criteria:** There are no records with differences between VIVID AND Datasphere within 2 decimal places 
# MAGIC - **Fail Criteria:** There is at least 1 record with a difference within 2 decimal places 
# MAGIC
# MAGIC #### Notes:
# MAGIC - This only validates data for the period of '2024-12'
# MAGIC - This compares an aggregate based on vcode, gl_account AND profit_center due to the differing granularity 
# MAGIC - Only compares SGR records

# COMMAND ----------

# MAGIC %run ../../../../common/properties

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

v001_reconcilliation = spark.sql(
  f'''
    WITH datasphere AS (
      select
        company_code
        , material
        , cost_center
        , profit_center
        , gl_account
        , datasource
        , sum(Reporting_QuantityL20) as Reporting_QuantityL20
        from vivid_dev_brz.sap_datasphere.`2vf_fi_reporting_001`
        where VCode LIKE '%V.001%'
        and Datasource in ( 'SGR', 'SGRJNLS')
        and YearPeriod = '2024012'
        group by all 
    )

    , vivid AS (
      SELECT 
          company_code
          , material
          , cost_center
          , profit_center
          , gl_account
          , datasource
          ,sum(amount_in_company_code_currency) AS amount_in_company_code_currency
          ,sum(amount_in_global_currency)       AS amount_in_global_currency
          ,sum(vcode_amount_local)              AS vcode_amount_local 
          ,sum(vcode_amount_group)              AS vcode_amount_group
          ,sum(volume_kg)                       AS volume_kg
          ,sum(volume_litres_l20)               AS volume_litres_l20
      from        
          {env_vars.silver_catalog}.fin_general_ledger.universal_journal_line_items
      WHERE
          last_day(posting_date) = '2024-12-31'
          AND vcode = 'V.001'
          and datasource in ( 'SGR', 'SGRJNLS')
      GROUP BY ALL 
    )

    , comparison AS (
      SELECT 
        datasphere.company_code
        ,datasphere.material
        ,datasphere.cost_center
        ,datasphere.profit_center
        ,datasphere.gl_account
        ,datasphere.datasource
        ,'#'
        ,datasphere.Reporting_QuantityL20 AS Reporting_QuantityL20
        ,vivid.vcode_amount_local AS viv__vcode_amount_local
        ,round(ABS(datasphere.Reporting_QuantityL20 - vivid.vcode_amount_local), 2) AS check__vcode_amount_local
        ,'#'
        ,datasphere.Reporting_QuantityL20 AS Reporting_QuantityL20
        ,vivid.vcode_amount_group AS viv__vcode_amount_group
        ,round(ABS(datasphere.Reporting_QuantityL20 - vivid.vcode_amount_group), 2) AS check__vcode_amount_group
        ,'#'
        ,vivid.amount_in_global_currency AS viv__amount_in_global_currency
        ,round(ABS(0 - vivid.amount_in_global_currency), 2) AS check__amount_in_global_currency
        ,'#'
        ,vivid.amount_in_company_code_currency AS viv__amount_in_company_code_currency
        ,round(ABS(0 - vivid.amount_in_company_code_currency), 2) AS check__amount_in_company_code_currency
      FROM 
        datasphere 

        LEFT JOIN vivid ON 
          datasphere.gl_account = vivid.gl_account
          AND datasphere.profit_center = vivid.profit_center
          AND datasphere.company_code = vivid.company_code
          AND datasphere.material = vivid.material
          AND datasphere.cost_center = vivid.cost_center   
    )

    SELECT
      *
    FROM
      comparison
    WHERE
      (check__vcode_amount_local > 0.1 OR check__vcode_amount_local IS NULL
      OR check__vcode_amount_group > 0.1 OR check__vcode_amount_group IS NULL
      or check__amount_in_global_currency > 0.1 OR check__amount_in_global_currency IS NULL
      or check__amount_in_company_code_currency > 0.1 OR check__amount_in_company_code_currency IS NULL)
  '''
)

v001_reconcilliation.display()
assert v001_reconcilliation.count() == 0, 'Differences between datasphere AND vivid found with V001 records!'

# COMMAND ----------

v002_reconcilliation = spark.sql(
  f'''
    WITH datasphere AS (
      SELECT
        month_end_date
        ,source 
        ,gl_account
        ,split(vcode, '-')[1] AS vcode
        ,profit_center
        ,company_code
        ,sum(amount_lc) AS amount_lc
        ,sum(amount_gc) AS amount_gc
        , sum(Reporting_QuantityLPG) as Reporting_QuantityLPG
      from
        {env_vars.bronze_catalog}.sap_datasphere.`2vf_fi_reporting_001`
      WHERE 1=1
        AND vcode LIKE '%V.002%'
        AND month_end_date = '2024-12-31'
        AND datasource LIKE '%SGR%'
        AND Reporting_QuantityLPG <> 0
      GROUP BY ALL
    )

    , vivid AS (
      SELECT 
        date(last_day(posting_date)) as month_end_date
        ,actual_plan_code 
        ,gl_account
        ,vcode
        ,company_code
        ,profit_center
        ,sum(amount_in_company_code_currency) AS amount_in_company_code_currency
        ,sum(amount_in_global_currency)       AS amount_in_global_currency
        ,sum(vcode_amount_local)              AS vcode_amount_local 
        ,sum(vcode_amount_group)              AS vcode_amount_group
      from
        {env_vars.silver_catalog}.fin_general_ledger.universal_journal_line_items
      WHERE
          last_day(posting_date) = '2024-12-31'
          AND vcode = 'V.002'
          and datasource in ( 'SGR', 'SGRJNLS')
      GROUP BY ALL 
    )

    , comparison AS (
      SELECT 
        datasphere.month_end_date 
        ,datasphere.source
        ,datasphere.gl_account
        ,datasphere.vcode
        ,datasphere.company_code
        ,datasphere.profit_center
        ,'#'
        ,datasphere.Reporting_QuantityLPG AS Reporting_QuantityLPG
        ,vivid.vcode_amount_local AS viv__vcode_amount_local
        ,round(ABS(datasphere.Reporting_QuantityLPG - vivid.vcode_amount_local), 2) AS check__vcode_amount_local
        ,'#'
        ,datasphere.Reporting_QuantityLPG AS Reporting_QuantityLPG
        ,vivid.vcode_amount_group AS viv__vcode_amount_group
        ,round(ABS(datasphere.Reporting_QuantityLPG - vivid.vcode_amount_group), 2) AS check__vcode_amount_group
        ,'#'
        ,vivid.amount_in_global_currency AS viv__amount_in_global_currency
        ,round(ABS(0 - vivid.amount_in_global_currency), 2) AS check__amount_in_global_currency
        ,'#'
        ,vivid.amount_in_company_code_currency AS viv__amount_in_company_code_currency
        ,round(ABS(0 - vivid.amount_in_company_code_currency), 2) AS check__amount_in_company_code_currency
      FROM 
        datasphere 

        LEFT JOIN vivid ON 
          datasphere.gl_account = vivid.gl_account
          AND datasphere.month_end_date = vivid.month_end_date
          AND datasphere.profit_center = vivid.profit_center
          AND datasphere.company_code = vivid.company_code
          AND datasphere.vcode = vivid.vcode
    )

    SELECT
      *
    FROM
      comparison
    WHERE
      (check__vcode_amount_local > 0.1 OR check__vcode_amount_local IS NULL
      OR check__vcode_amount_group > 0.1 OR check__vcode_amount_group IS NULL
      or check__amount_in_global_currency > 0.1 OR check__amount_in_global_currency IS NULL
      or check__amount_in_company_code_currency > 0.1 OR check__amount_in_company_code_currency IS NULL)
  '''
)

v002_reconcilliation.display()
assert v002_reconcilliation.count() == 0, 'Differences between datasphere AND vivid found with V002 records!'