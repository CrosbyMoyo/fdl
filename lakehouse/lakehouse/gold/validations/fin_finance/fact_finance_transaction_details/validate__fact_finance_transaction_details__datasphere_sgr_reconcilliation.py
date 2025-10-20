# Databricks notebook source
# MAGIC %md
# MAGIC ## Validate Fact Finance Transaction Details: Actual & Plan Datasphere Reconcilliation
# MAGIC #### Purpose:
# MAGIC This notebook compares the values of the `gld.fin_finance.fact_finance_transaction_details` to the `brz.sap_datasphere.2vf_fi_reporting_001` and compares the volume and currency amounts for the actual and plan transactions greater than a variance of 0.1.
# MAGIC
# MAGIC #### Pass/Fail Criteria:
# MAGIC - **Pass Criteria:** There are no records with differences between VIVID AND Datasphere greater than a variance of 0.1
# MAGIC - **Fail Criteria:** There is at least 1 record with a difference greater than a variance of 0.1
# MAGIC
# MAGIC #### Notes:
# MAGIC - Set the month from and to in the variables to run the query correctly
# MAGIC - This only compares SGR and does not reconcile S4HANA, SAC or HFM or data 
# MAGIC
# MAGIC #### Expected Result [14/05/2025]:
# MAGIC - 3 rows not reconciling they are due to several issues see [bug #1844](https://dev.azure.com/VivoEnergy/Azure%20SAP%20Data%20Reporting/_sprints/taskboard/Vivid%20Platform%20Team/Azure%20SAP%20Data%20Reporting/Sprint%2010?System.AssignedTo=ahmet.cihan%40vivoenergy.com%2Cnicholas.ward%40vivoenergy.com&workitem=1844)

# COMMAND ----------

# MAGIC %run ../../../../common/properties

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

# Set Month From and Month To for the query filters
fact_date_from = '2024-12-01'
fact_date_to = '2024-12-31'

# Run the reconciliation query below
validation = spark.sql(
  f'''
    WITH datasphere AS (
      select 
        company_code
        , gl_account
        , profit_center
        , document_type
        , datasource
        , vcodes as vcode
        , key
        , sum(ds_amount_lc) as ds_amount_lc
        , sum(ds_amount_gc) as ds_amount_gc
        , sum(ds_volume_litres) as ds_volume_litres
        , sum(ds_volume_kg) as ds_volume_kg
        , sum(ds_vcode_amount_local) as ds_vcode_amount_local
        , sum(ds_vcode_amount_group) as ds_vcode_amount_group        
    from
        (
        select 
            ifnull(Company_Code, '¬¬') as company_code
            , ifnull(GL_Account, '¬¬') as gl_account
            , ifnull(profit_center, '¬¬') as profit_center
            , ifnull(DOCTY, '¬¬') as document_type
            , ifnull(datasource, '¬¬') as datasource
            , ifnull(split(VCode, '-')[1], '¬¬') as vcodes
            , profit_center || '|' || company_code || '|' || gl_account || '|' || document_type || '|' || datasource || '|' || vcodes as key
            , case when VCode like '%V%' then 0 when Amount_LC IS NULL then 0 else try_cast(Amount_LC as decimal(20,8))end as ds_amount_lc
            , case when VCode like '%V%' then 0 when Amount_GC IS NULL then 0 else try_cast(Amount_GC as decimal(20,8)) end  as ds_amount_gc    
            , case when VCode like '%V%' then 0 when Reporting_QuantityL20 IS NULL then 0 else try_cast(Reporting_QuantityL20 as decimal(20,8)) end as ds_volume_litres
            , case when VCode like '%V%' then 0 when Reporting_QuantityLPG IS NULL then 0 else try_cast(Reporting_QuantityLPG as decimal(20,8)) end as ds_volume_kg
            , case 
              when VCode like '%V.001%' then Reporting_QuantityL20 
              when vcode like '%V.002%' then Reporting_QuantityLPG
              else Amount_LC 
            end as ds_vcode_amount_local
            , case 
              when VCode like '%V.001%' then Reporting_QuantityL20 
              when vcode like '%V.002%' then Reporting_QuantityLPG
              else Amount_GC 
            end as ds_vcode_amount_group
        from {env_vars.bronze_catalog}.sap_datasphere.`2vf_fi_reporting_001`
        where Datasource in ('SGR', 'SGRJNLS')
          and Month_End_Date between '{fact_date_from}' and '{fact_date_to}'
        ) 
    group by all
    order by 1,2,3,4
    ),

    vivid AS (
      select 
        company_code
        , gl_account
        , profit_center
        , document_type
        , datasource
        , vcode
        , balance_sheet_account_flag
        , key
        , sum(vvd_amount_lc) as vvd_amount_lc
        , sum(vvd_amount_gc) as vvd_amount_gc
        , sum(vvd_amount_gc_plan) as vvd_amount_gc_plan
        , sum(vvd_amount_gc_monthly_end) as vvd_amount_gc_monthly_end
        , sum(vvd_volume_litres) as vvd_volume_litres
        , sum(vvd_volume_kg) as vvd_volume_kg
        , sum(vvd_vcode_amount_local) as vvd_vcode_amount_local
        , sum(vvd_vcode_amount_group) as vvd_vcode_amount_group
    from
    (
    select 
      ifnull(nullif(comp.company_code, ''), '¬¬') as company_code
    , ifnull(nullif(gl.gl_account, ''), '¬¬') as gl_account
    , ifnull(nullif(pc.profit_center, ''), '¬¬') as profit_center
    , ifnull(nullif(fact.document_type, ''), '¬¬') as document_type
    , ifnull(nullif(datasource.datasource_name, ''), '¬¬') as datasource
     , ifnull(nullif(vcode.vcode, ''), '¬¬') as vcode
    , gl.balance_sheet_account_flag  as balance_sheet_account_flag -- Balance Sheet accounts use wrong exchange rate in Datasphere
    , profit_center || '|' || company_code || '|' || gl_account || '|' || document_type || '|' || datasource || '|' || vcode as key 
    , amount_local_currency as vvd_amount_lc
    , amount_group_currency as vvd_amount_gc
    , amount_group_currency_plan_rate as vvd_amount_gc_plan
    , amount_group_currency_month_end as vvd_amount_gc_monthly_end
    , volume_litres_l20 as vvd_volume_litres
    , volume_kg as vvd_volume_kg
    , vcode_amount_local as vvd_vcode_amount_local
    , vcode_amount_group as vvd_vcode_amount_group
    from {env_vars.gold_catalog}.fin_finance.fact_finance_transaction_details fact
    left join  {env_vars.gold_catalog}.fin_general_ledger.dim_gl_account gl
    on fact.gl_account_skey = gl.gl_account_skey
    left join  {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code comp
    on fact.company_code_skey = comp.company_code_skey
    left join  {env_vars.gold_catalog}.fin_controlling.dim_profit_center pc 
      on fact.profit_center_skey = pc.profit_center_skey
    left join  {env_vars.gold_catalog}.fin_general_ledger.dim_vcodes vcode
    on fact.vcode_skey = vcode.vcode_skey
    left join  {env_vars.gold_catalog}.ca_cross_application_components.dim_datasource datasource
    on fact.datasource_skey = datasource.datasource_skey
    where datasource.datasource_name IN ('SGR', 'SGRJNLS')
      and fact.date_key between '{fact_date_from}' and '{fact_date_to}'
    )
    group by all
    order by 1,2,3,4,5
    ),

    comparison AS (
      SELECT 
        datasphere.company_code,
        datasphere.gl_account,
        datasphere.profit_center,
        datasphere.vcode,
        datasphere.key,
        vivid.balance_sheet_account_flag,
        '#',
        vivid.vvd_amount_gc,
        datasphere.ds_amount_gc,
        ROUND(ABS(datasphere.ds_amount_gc - vivid.vvd_amount_gc), 2) AS check__amount_gc,
        '#',
        vivid.vvd_amount_lc,
        datasphere.ds_amount_lc, 
        ROUND(ABS(datasphere.ds_amount_lc - vivid.vvd_amount_lc), 2) AS check__amount_lc,
         '#',       
        datasphere.ds_volume_litres,
        vivid.vvd_volume_litres,
        ROUND(ABS(datasphere.ds_volume_litres - vivid.vvd_volume_litres), 2) AS check__volume_litres,
        '#',        
         datasphere.ds_volume_kg,
        vivid.vvd_volume_kg,
        ROUND(ABS(datasphere.ds_volume_litres - vivid.vvd_volume_litres), 2) AS check__volume_kg, 
         '#',       
        datasphere.ds_vcode_amount_local,
        vivid.vvd_vcode_amount_local,
        ROUND(ABS(datasphere.ds_vcode_amount_local - vivid.vvd_vcode_amount_local), 2) AS check__vcode_amount_local,
         '#',           
        datasphere.ds_vcode_amount_group,
        vivid.vvd_vcode_amount_group,
        ROUND(ABS(datasphere.ds_vcode_amount_group - vivid.vvd_vcode_amount_group), 2) AS check__vcode_amount_group   
      FROM datasphere
      LEFT JOIN vivid 
        ON datasphere.key = vivid.key
    )

    SELECT *
    FROM comparison
    WHERE  
       (
       (check__amount_lc > 0.1 OR check__amount_lc IS NULL)
       or (check__amount_gc > 0.1 OR check__amount_gc IS NULL)   
       or (check__volume_litres > 0.1 OR check__volume_litres IS NULL)   
       or (check__volume_kg > 0.1 OR check__volume_kg IS NULL)  
       or (check__vcode_amount_local > 0.1 OR check__vcode_amount_local IS NULL)  
       or (check__vcode_amount_group > 0.1 OR check__vcode_amount_group IS NULL) 
       ) 
       and (balance_sheet_account_flag = false or balance_sheet_account_flag is null) -- Datasphere model uses wrong exhange rate for Balance Sheet accounts     
 
  '''
)

validation.display()

# COMMAND ----------

assert validation.isEmpty(), 'Differences between datasphere AND vivid found'