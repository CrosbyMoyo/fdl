# Databricks notebook source
# MAGIC %md
# MAGIC ## Validate Fact Finance Transaction Details: Actual & Plan Datasphere Reconcilliation
# MAGIC #### Purpose:
# MAGIC This notebook compares the values of the `gld.fin_finance.fact_finance_transaction_details` to the `brz.sap_datasphere.2vf_fi_reporting_001` and compares the volume and currency amounts for the actual and plan transactions within a 0.1 variance.
# MAGIC
# MAGIC #### Pass/Fail Criteria:
# MAGIC - **Pass Criteria:** There are no records with differences between VIVID AND Datasphere greater than a variance of 0.1
# MAGIC - **Fail Criteria:** There is at least 1 record with a difference greate than a variance of 0.1
# MAGIC
# MAGIC #### Notes:
# MAGIC - Set the month from and to in the variables to run the query correctly
# MAGIC - This only compares S4HANA and does not reconcile SAC, HFM or SGR data 
# MAGIC
# MAGIC #### Expected Result [22/04/2025]
# MAGIC - Currently this is returning just two rows with vcode = P.123.1. See [bug #1225](https://dev.azure.com/VivoEnergy/Azure%20SAP%20Data%20Reporting/_sprints/taskboard/Vivid%20Platform%20Team/Azure%20SAP%20Data%20Reporting/Sprint%208?System.AssignedTo=%40me).
# MAGIC - There are also 8000+ rows with v003 that are not appearing in VIVD yet See [bug #1260](https://dev.azure.com/VivoEnergy/Azure%20SAP%20Data%20Reporting/_sprints/taskboard/Vivid%20Platform%20Team/Azure%20SAP%20Data%20Reporting/Sprint%208?System.AssignedTo=%40me&workitem=1260). This has been commented out until that is resolved.

# COMMAND ----------

# MAGIC %run ../../../../common/properties

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

# Set Month From and Month To for the query filters
lv_date_from = '2024-12-01'
lv_date_to = '2024-12-31'

# Run the reconciliation query below
validation = spark.sql(
  f'''
    WITH datasphere AS (
      select 
        company_code
        , gl_account
        , vcodes as vcode
        , source
        , date
        , key
        , profit_center
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
            , ifnull(split(VCode, '-')[1], '¬¬') as vcodes
            , ifnull(source, '¬¬') as source
            , ifnull(budat,'1900-01-01'::date)::date as date
            , ifnull(profit_center, '¬¬') as profit_center
            , profit_center || '|' || company_code || '|' || gl_account || '|' || vcodes || '|' || source || '|' || date as key
            , case when VCode like '%V%' then 0 else try_cast(Amount_LC as decimal(20,8))end as ds_amount_lc
            , case when VCode like '%V%' then 0 else try_cast(Amount_GC as decimal(20,8)) end  as ds_amount_gc    
            , case when VCode like '%V%' then 0 else try_cast(Reporting_QuantityL20 as decimal(20,8)) end as ds_volume_litres
            , case when VCode like '%V%' then 0 else try_cast(Reporting_QuantityLPG as decimal(20,8)) end as ds_volume_kg
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
        where Datasource in ('S/4HANA')
          and budat between '{lv_date_from}' and '{lv_date_to}'
        ) 
    group by all
    order by 1,2,3,4,5
    ),

    vivid AS (
      select 
        company_code
        , gl_account
        , vcode
        , source
        , date
        , profit_center
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
    , ifnull(nullif(vcode.vcode, ''), '¬¬') as vcode
    , ifnull(nullif(pc.profit_center, ''), '¬¬') as profit_center
    , ifnull(upper(CASE WHEN left(fact.actual_plan_code, 4) = 'PLAN' THEN left(fact.actual_plan_code, 4) else fact.actual_plan_code end), '¬¬') as source   
    , CASE WHEN left(fact.actual_plan_code, 4) = 'PLAN' then ifnull((left(date_key, 8) || '01')::date, '1900-01-01'::date) else ifnull(date_key, '1900-01-01'::date) end as date
    , gl.balance_sheet_account_flag -- Balance Sheet accounts use wrong exchange rate in Datasphere
    , profit_center || '|' || company_code || '|' || gl_account || '|' || vcode || '|' || source || '|' || date as key
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
    where 1=1
    and datasource.datasource_name = 'S4HANA'
    )
    where date between '{lv_date_from}' and '{lv_date_to}'
    group by all
    order by 1,2,3,4,5
    ),

    comparison AS (
      SELECT 
        datasphere.company_code,
        datasphere.gl_account,
        datasphere.vcode,
        datasphere.date,
        datasphere.profit_center,
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
        ROUND(ABS(datasphere.ds_volume_kg - vivid.vvd_volume_kg), 2) AS check__volume_kg, 
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
    WHERE 1=1
       and 
       (
       (check__amount_lc > 0.1 OR check__amount_lc IS NULL)
       or (check__amount_gc > 0.1 OR check__amount_gc IS NULL)   
       or (check__volume_litres > 0.1 OR check__volume_litres IS NULL)   
       or (check__volume_kg > 0.1 OR check__volume_kg IS NULL)  
       or (check__vcode_amount_local > 0.1 OR check__vcode_amount_local IS NULL)  
       or (check__vcode_amount_group > 0.1 OR check__vcode_amount_group IS NULL) 
       ) 
       and (balance_sheet_account_flag = false or balance_sheet_account_flag is null) -- Datasphere model uses wrong exhange rate for Balance Sheet accounts     
       and vcode not like '%V.003%'    
  '''
)

validation.display()

# COMMAND ----------

assert validation.isEmpty(), 'Differences between datasphere AND vivid found'