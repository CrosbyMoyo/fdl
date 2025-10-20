# Databricks notebook source
# MAGIC %md
# MAGIC ## Validate: Universal Journal Line Items S4HANA Datasphere Reconcilliation
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
# MAGIC - Only compares S4HANA records

# COMMAND ----------

# MAGIC %run ../../../../common/properties

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

v001_reconcilliation = spark.sql(
  f'''
    WITH datasphere AS (
      SELECT
        month_end_date
        ,source 
        ,gl_account
        ,split(vcode, '-')[1] AS vcode
        ,profit_center
        ,company_code
        ,sum(CAST(amount_lc AS DECIMAL(38,18))) AS amount_lc
        ,sum(CAST(amount_gc AS DECIMAL(38,18))) AS amount_gc
        ,sum(CAST(Reporting_QuantityL20 AS DECIMAL(38,18))) AS Reporting_QuantityL20
      from
        vivid_dev_brz.sap_datasphere.`2vf_fi_reporting_001`
      WHERE 1=1
        AND vcode LIKE '%V.001%'
        AND month_end_date = '2024-12-31'
        AND datasource LIKE '%HANA%'
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
        vivid_933_slv.fin_general_ledger.universal_journal_line_items
      WHERE
          last_day(posting_date) = '2024-12-31'
          AND vcode = 'V.001'
          AND datasource IN ('S4HANA')
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
        ,datasphere.Reporting_QuantityL20 AS ds__Reporting_QuantityL20
        ,vivid.vcode_amount_local AS viv__vcode_amount_local
        ,round(ABS(datasphere.Reporting_QuantityL20 - vivid.vcode_amount_local), 2) AS check__vcode_amount_local
        ,'#'
        ,datasphere.Reporting_QuantityL20 AS ds__Reporting_QuantityL20
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
        ,sum(CAST(amount_lc AS DECIMAL(38,18))) AS amount_lc
        ,sum(CAST(amount_gc AS DECIMAL(38,18))) AS amount_gc
        ,sum(CAST(Reporting_QuantityLPG AS DECIMAL(38,18))) AS Reporting_QuantityLPG
      from
        vivid_dev_brz.sap_datasphere.`2vf_fi_reporting_001`
      WHERE 1=1
        AND vcode LIKE '%V.002%'
        AND month_end_date = '2024-12-31'
        AND datasource LIKE '%HANA%'
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
        vivid_933_slv.fin_general_ledger.universal_journal_line_items
      WHERE
          last_day(posting_date) = '2024-12-31'
          AND vcode = 'V.002'
          AND datasource IN ('S4HANA')
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
        ,datasphere.Reporting_QuantityLPG AS ds__Reporting_QuantityLPG
        ,vivid.vcode_amount_local AS viv__vcode_amount_local
        ,round(ABS(datasphere.Reporting_QuantityLPG - vivid.vcode_amount_local), 2) AS check__vcode_amount_local
        ,'#'
        ,datasphere.Reporting_QuantityLPG AS ds__Reporting_QuantityLPG
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