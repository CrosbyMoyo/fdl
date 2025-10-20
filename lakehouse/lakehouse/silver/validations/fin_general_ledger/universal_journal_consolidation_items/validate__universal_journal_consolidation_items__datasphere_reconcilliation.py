# Databricks notebook source
# MAGIC %md
# MAGIC ## Validate: Universal Journal Line Items Consolidation Datasphere Reconcilliation
# MAGIC #### Purpose:
# MAGIC
# MAGIC
# MAGIC #### Pass/Fail Criteria:
# MAGIC - **Pass Criteria:**  
# MAGIC - **Fail Criteria:** 
# MAGIC
# MAGIC #### Notes:
# MAGIC

# COMMAND ----------

# MAGIC %run ../../../../common/properties

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

reconcilliation = spark.sql(
    f'''
            select
            *
            from
            (
                select
                ds.*,
                vvd_amount_lc,
                vvd_amount_gc,
                vvd_quantity,
                ds_quantity - vvd_quantity as var_quantity,
                ds_amount_lc - vvd_amount_lc as var_amount_lc,
                ds_amount_gc - vvd_amount_gc as var_amount_gc,
                case
                    when abs(try_divide(var_quantity, vvd_quantity)) > 0.1 then true
                    when var_quantity > 0.1 then true
                    else false
                end as err_quantity_flag,
                case
                    when abs(try_divide(var_amount_lc, vvd_amount_lc)) > 0.1 then true
                    when var_amount_lc > 0.1 then true
                    else false
                end as err_amount_lc_flag,
                case
                    when abs(try_divide(var_amount_gc, vvd_amount_gc)) > 0.1 then true
                    when var_amount_gc > 0.1 then true
                    else false
                end as err_amount_gc_flag
                from
                (
                    select
                    partner_unit,
                    document_type,
                    company_code,
                    material,
                    cost_center,
                    profit_center,
                    financial_statement_item,
                    gl_account,
                    datasource,
                    key,
                    sum(quantity) as ds_quantity,
                    sum(ds_amount_lc) as ds_amount_lc,
                    sum(ds_amount_gc) as ds_amount_gc
                    from
                    (
                        select
                        ifnull(RBUPTR, '') as partner_unit,
                        ifnull(DOCTY, '') as document_type,
                        ifnull(Company_Code, '') as company_code,
                        ifnull(Material, '') as material,
                        ifnull(Profit_Center, '') as profit_center,
                        ifnull(Cost_Center, '') as cost_center,
                        ifnull(RITEM, '') as financial_statement_item,
                        ifnull(GL_Account, '') as gl_account,
                        ifnull(Datasource, '') as datasource,
                        IFNULL(RBUPTR, '') || '|' || IFNULL(Company_Code, '') || '|' || IFNULL(DOCTY, '') || '|' || IFNULL(Material, '') || '|' || IFNULL(Cost_Center, '') || '|' || IFNULL(Profit_Center, '') || '|' || IFNULL(RITEM, '') || '|' || IFNULL(GL_Account, '') || '|' || IFNULL(Datasource, '') as key,
                        Reporting_QuantityL20 as quantity,
                        Amount_LC as ds_amount_lc,
                        Amount_GC as ds_amount_gc
                        from
                        { env_vars.bronze_catalog }.sap_datasphere.`2vf_fi_reporting_001`
                        where
                        VCode not like '%V.%'
                        and Datasource in ('SGR', 'SGRJNLS')
                        and YearPeriod = '2024012' --and Company_Code = 'BW01'
                    )
                    group by
                    partner_unit,
                    document_type,
                    company_code,
                    material,
                    cost_center,
                    profit_center,
                    financial_statement_item,
                    gl_account,
                    datasource,
                    key
                    order by
                    partner_unit,
                    document_type,
                    company_code,
                    material,
                    cost_center,
                    profit_center,
                    financial_statement_item,
                    gl_account,
                    datasource
                ) ds
                left join (
                    select
                    partner_unit,
                    document_type,
                    company_code,
                    material,
                    cost_center,
                    profit_center,
                    financial_statement_item,
                    gl_account,
                    datasource,
                    key,
                    sum(vvd_quantity) as vvd_quantity,
                    sum(vvd_amount_lc) as vvd_amount_lc,
                    sum(vvd_amount_gc) as vvd_amount_gc
                    from
                    (
                        select
                        -- Dimension
                        -- Local currency
                        -- YearPeriod
                        -- Financial Statement Item
                        -- Posting Level
                        -- Document Type
                        -- Material Number
                        -- Cost Center
                        -- Profit Center
                        -- COArea
                        -- DataSource
                        -- Group Currency
                        -- Chart of Accounts
                        -- Month End Date
                        ifnull(partner_unit, '¬¬') as partner_unit,
                        ifnull(document_type, '¬¬') as document_type,
                        ifnull(company_code, '¬¬') as company_code,
                        ifnull(material_number, '¬¬') as material,
                        ifnull(profit_center, '¬¬') as profit_center,
                        ifnull(cost_center, '¬¬') as cost_center,
                        ifnull(financial_statement_item, '¬¬') as financial_statement_item,
                        ifnull(gl_account, '¬¬') as gl_account,
                        ifnull(datasource, '¬¬') as datasource,
                        IFNULL(partner_unit, '') || '|' || IFNULL(company_code, '') || '|' || IFNULL(document_type, '') || '|' || IFNULL(material_number, '') || '|' || IFNULL(cost_center, '') || '|' || IFNULL(profit_center, '') || '|' || IFNULL(financial_statement_item, '') || '|' || IFNULL(gl_account, '') || '|' || IFNULL(datasource, '') as key,
                        quantity as vvd_quantity,
                        amount_in_group_currency as vvd_amount_gc,
                        amount_in_local_currency as vvd_amount_lc
                        from
                        { env_vars.silver_catalog }.fin_general_ledger.universal_journal_consolidation_items -- where v_code not like 'PR.%'
                        where
                        month_end_date = '2024-12-31'
                    )
                    group by
                    partner_unit,
                    document_type,
                    company_code,
                    material,
                    cost_center,
                    profit_center,
                    financial_statement_item,
                    gl_account,
                    datasource,
                    key
                    order by
                    partner_unit,
                    document_type,
                    company_code,
                    material,
                    cost_center,
                    profit_center,
                    financial_statement_item,
                    gl_account,
                    datasource
                ) vvd on ds.key = vvd.key
            )
            where
            (
                err_amount_gc_flag = true
                or err_amount_lc_flag = true
                or err_quantity_flag = true
                or vvd_amount_lc is null
            ) -- (datasource = 'SGRJNLS')
            -- --and key like 'TN01%'
            -- -- and key like '%2024-12%'
            and (
                ds_amount_lc <> 0
                and ds_amount_gc <> 0
            ) -- and document_type = '2E'
            -- ICTN04|TN01|2K|000000000000100672||TN01BBFU01|61010_1|0610100011|SGR
            ;
    '''
)

reconcilliation.display()

assert reconcilliation.count() == 0