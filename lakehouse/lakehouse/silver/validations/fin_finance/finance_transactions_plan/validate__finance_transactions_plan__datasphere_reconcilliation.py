# Databricks notebook source
# MAGIC %md
# MAGIC ## Validate Finance Transactions Plan: Datasphere Reconcilliation
# MAGIC #### Purpose:
# MAGIC This notebook compares the values of the `slv.fin_finance.finance_transactions_plans` to the `brz.sap_datasphere.2vf_fi_reporting_001` comparing the following measures:
# MAGIC - amount local currency 
# MAGIC - volume litres 
# MAGIC - vcode amount local 
# MAGIC
# MAGIC #### Pass/Fail Criteria:
# MAGIC - **Pass Criteria:** There are no records with differences between VIVID AND Datasphere than a variance of 0.1
# MAGIC - **Fail Criteria:** There is at least 1 record with a difference than a variance of 0.1
# MAGIC
# MAGIC #### Notes:
# MAGIC - Set the month from and to in the variables to run the query correctly
# MAGIC - This only compares SAC and does not reconcile S4HANA, HFM or SGR data 
# MAGIC
# MAGIC #### Expected Result [30/04/2025]:
# MAGIC - (NW) Currently there are no reconcilliation issues returning 
# MAGIC

# COMMAND ----------

# MAGIC %run ../../../../common/properties

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

lv_date_from = '2024-12-01'
lv_date_to = '2024-12-31'

# Run the reconciliation query below
validation = spark.sql(
  f'''
    WITH datasphere AS (
      select 
        planyear
        , company_code
        , gl_account
        , vcodes as vcode
        , date
        , key
        , profit_center
        , sum(ds_amount_lc) as ds_amount_lc
        , sum(ds_volume_litres) as ds_volume_litres
        , sum(ds_volume_kg) as ds_volume_kg
        , sum(ds_vcode_amount_local) as ds_vcode_amount_local
    from
        (
        select 
            planyear 
            , ifnull(Company_Code, '¬¬') as company_code
            , ifnull(GL_Account, '¬¬') as gl_account
            , ifnull(split(VCode, '-')[1], '¬¬') as vcodes
            , ifnull(budat,'1900-01-01'::date)::date as date
            , ifnull(profit_center, '¬¬') as profit_center
            , planyear || '|' || profit_center || '|' || company_code || '|' || gl_account || '|' || vcodes || '|' || date as key
            , case 
              when VCode like '%V%' then 0
              else try_cast(Amount_LC as decimal(20,8)) 
            end as ds_amount_lc
            , case 
              when VCode like '%V%' then 0
              else try_cast(Amount_GC as decimal(20,8))
              end  as ds_amount_gc
            , case 
              when VCode like '%V%' then 0
              when vcode like '%P.111.111' then Reporting_QuantityL20 
              else try_cast(Reporting_QuantityL20 as decimal(20,8)) 
              end as ds_volume_litres
            , case 
              when VCode like '%V%' then 0
              else try_cast(Reporting_QuantityLPG as decimal(20,8)) 
            end as ds_volume_kg
            , case 
              when VCode like '%V.001%' then Reporting_QuantityL20 
              when vcode like '%V.002%' then Reporting_QuantityLPG
              else Amount_LC
            end as ds_vcode_amount_local
            , case 
              when VCode like '%V%' then 0
              else Amount_GC 
            end as ds_vcode_amount_group
        from {env_vars.bronze_catalog}.sap_datasphere.`2vf_fi_reporting_001`
        where Datasource in ('PLANSAC')
          and budat between '{lv_date_from}' and '{lv_date_to}'
        )
    group by all
    order by all
    ),

    vivid AS (
      select 
        planyear
        , company_code
        , gl_account
        , vcode
        , date 
        , profit_center
        , key
        , sum(vvd_amount_lc)          as vvd_amount_lc
        , sum(vvd_volume_litres)      as vvd_volume_litres
        , sum(vvd_volume_kg)          as vvd_volume_kg
        , sum(vvd_vcode_amount_local) as vvd_vcode_amount_local
    from
    (
      select 
        CAST(substring(plan_version, 6, 4) AS INT) AS planyear
        ,  ifnull(nullif(company_code, ''), '¬¬') as company_code
        , ifnull(nullif(gl_account, ''), '¬¬') as gl_account
        , ifnull(nullif(vcode, ''), '¬¬') as vcode
        , ifnull(nullif(profit_center, ''), '¬¬') as profit_center
        , ifnull((left(date_key, 8) || '01')::date, '1900-01-01'::date) as date
        , CAST(substring(plan_version, 6, 4) AS INT) || '|' || profit_center || '|' || company_code || '|' || gl_account || '|' || vcode || '|' || date as key
        , amount_local_currency as vvd_amount_lc
        , volume_litres_l20 as vvd_volume_litres
        , volume_kg as vvd_volume_kg
        , vcode_amount_local as vvd_vcode_amount_local
      from 
        {env_vars.silver_catalog}.fin_finance.finance_transactions_plan fact
    )
          where date between '{lv_date_from}' and '{lv_date_to}'
    group by all
    order by all
    ),

    comparison AS (
      SELECT 
        datasphere.planyear,
        datasphere.company_code,
        datasphere.gl_account,
        datasphere.vcode,
        datasphere.date,
        datasphere.profit_center,
        datasphere.key,
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
        ROUND(ABS(datasphere.ds_vcode_amount_local - vivid.vvd_vcode_amount_local), 2) AS check__vcode_amount_local
      FROM datasphere
      LEFT JOIN vivid
        ON datasphere.key = vivid.key
    )

    SELECT *
    FROM comparison
    WHERE 1=1
    and (
      check__amount_lc <> 0 or check__amount_lc is null 
      or check__vcode_amount_local <> 0 or check__vcode_amount_local is null 
      or check__volume_litres <> 0 or check__volume_litres is null 
    )
  '''
)

validation.display()

# COMMAND ----------

assert validation.isEmpty(), 'Differences between datasphere AND vivid found'