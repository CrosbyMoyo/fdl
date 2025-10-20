# Databricks notebook source
# MAGIC %md
# MAGIC ## CSKS Bronze to Silver
# MAGIC
# MAGIC Generic Notebook moves the `{bronze}.fivetran_s4p.cosp` data to a staging table defined in the metadata.
# MAGIC
# MAGIC This notebook further refines that data, and adds the payload fingerprint.  Then merges the data into `{silver}.fin_controlling.cosp` table.

# COMMAND ----------

# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.capex.yaml"
logger.log.info(f'"metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", include_schemaversion=True)}'

# COMMAND ----------

# DBTITLE 1,AUFK - Order Master Data
spark.sql(
    f'''
        SELECT
            od.order_number
            ,od.order_type
            ,od.order_category
            ,od.created_on
            ,od.description
            ,od.processing_group
            ,agd.activity_type_name AS subtype_name
            ,od.company_code
            ,od.plant
            ,od.phase0_order_created
            ,od.phase1_order_released
            ,od.controlling_area
            ,od.phase2_order_completed
            ,od.phase3_order_closed
            ,od.order_currency
            ,od.object_number
            ,od.profit_center
            ,od.__etl_keys_fprint
            ,od.__etl_effective_from
            ,od.__etl_effective_to
            ,od.__etl_is_active
            ,od.__etl_is_deleted
        FROM 
            {env_vars.silver_catalog}.fin_controlling.order_master_data AS od
        LEFT JOIN 
            {env_vars.silver_catalog}.fin_general_ledger.allocation_group_descriptions AS agd
        ON 
            od.processing_group = agd.processing_group AND od.controlling_area = agd.controlling_area
        WHERE 
            od.order_type IN ('Z500', 'Z600') --AUART maps to order_type
    '''
).createOrReplaceTempView('order_master_data')

# COMMAND ----------

# DBTITLE 1,TBP0L - Budget Planning
spark.sql(
    f'''
        SELECT 
           bp.client
           ,bp.currency
           ,bp.ledger
        FROM 
            {env_vars.silver_catalog}.fin_controlling.budget_planning_ledger AS bp 
        WHERE  
            bp.client = '200' AND 
            bp.currency != 'USD'
    '''
).createOrReplaceTempView('budget_planning')


# COMMAND ----------

spark.sql(
    f'''
        SELECT 
            bpja.ledger,
            bpja.object_number,
            bpja.fiscal_year,
            SUM(
                CASE 
                    WHEN bpja.value_type = '41' AND bpja.budget_type = 'KBUD' 
                    THEN bpja.annual_value_ledger_currency 
                    ELSE 0 
                END
            ) AS budget_usd,

            SUM(
                CASE 
                    WHEN bpja.value_type = '41' AND bpja.budget_type = 'KBUD' 
                    THEN bpja.annual_value_transaction_currency 
                    ELSE 0 
                END
            ) AS budget_local,

            SUM(
                CASE 
                    WHEN bpja.value_type = '42' 
                    THEN bpja.annual_value_transaction_currency 
                    ELSE 0 
                END
            ) AS allocated_local

        FROM 
            {env_vars.silver_catalog}.fin_controlling.totals_for_budget_and_plan AS bpja
        WHERE 
            bpja.client = '200' AND 
            bpja.value_type IN ('41', '42')
        GROUP BY 
            bpja.ledger, 
            bpja.object_number, 
            bpja.fiscal_year
    '''
).createOrReplaceTempView('total_records_1')


# COMMAND ----------

spark.sql(
    f'''
        SELECT 
            bpja.ledger,
            bpja.object_number,
            bpja.fiscal_year,
            SUM(bpja.annual_value_ledger_currency) AS budget_usd
        FROM {env_vars.silver_catalog}.fin_controlling.totals_for_budget_and_plan bpja
        WHERE 
            bpja.client = '200' AND 
            bpja.value_type = '41' AND 
            bpja.budget_type = 'KBUD' AND 
            bpja.ledger = '0002'
        GROUP BY 
            bpja.ledger,
            bpja.object_number,
            bpja.fiscal_year
    '''
).createOrReplaceTempView('total_records_2')


# COMMAND ----------

spark.sql(
    f'''
        SELECT 
            rq.object_number
            ,rq.fiscal_year
            ,SUM(CASE WHEN rq.value_type IN ('04', '11') THEN rq.value_in_obj_crcy1 + rq.value_in_obj_crcy2 + rq.value_in_obj_crcy3 + rq.value_in_obj_crcy4 + rq.value_in_obj_crcy5 + rq.value_in_obj_crcy6 + rq.value_in_obj_crcy7 + rq.value_in_obj_crcy8 + rq.value_in_obj_crcy9 + rq.value_in_obj_crcy10 + rq.value_in_obj_crcy11 + rq.value_in_obj_crcy12 ELSE 0 END) AS actuals_local
            ,SUM(CASE WHEN rq.value_type IN (21,22,23,24) THEN rq.value_in_obj_crcy1 + rq.value_in_obj_crcy2 + rq.value_in_obj_crcy3 + rq.value_in_obj_crcy4 + rq.value_in_obj_crcy5 + rq.value_in_obj_crcy6 + rq.value_in_obj_crcy7 + rq.value_in_obj_crcy8 + rq.value_in_obj_crcy9 + rq.value_in_obj_crcy10 + rq.value_in_obj_crcy11 + rq.value_in_obj_crcy12 ELSE 0 END) AS committed_local
            ,SUM(CASE WHEN rq.value_type IN ('04', '11') THEN rq.valcoarea_crcy1 + rq.valcoarea_crcy2 + rq.valcoarea_crcy3 + rq.valcoarea_crcy4 + rq.valcoarea_crcy5 + rq.valcoarea_crcy6 + rq.valcoarea_crcy7 + rq.valcoarea_crcy8 + rq.valcoarea_crcy9 + rq.valcoarea_crcy10 + rq.valcoarea_crcy11 + rq.valcoarea_crcy12 ELSE 0 END) AS actuals_usd
            ,SUM(CASE WHEN rq.value_type IN (21,22,23,24) THEN rq.valcoarea_crcy1 + rq.valcoarea_crcy2 + rq.valcoarea_crcy3 + rq.valcoarea_crcy4 + rq.valcoarea_crcy5 + rq.valcoarea_crcy6 + rq.valcoarea_crcy7 + rq.valcoarea_crcy8 + rq.valcoarea_crcy9 + rq.valcoarea_crcy10 + rq.valcoarea_crcy11 + rq.valcoarea_crcy12 ELSE 0 END) AS committed_usd
        FROM 
            {env_vars.silver_catalog}.fin_controlling.cost_totals_for_external_postings rq
        GROUP BY 
            rq.object_number
            ,rq.fiscal_year
    '''
).createOrReplaceTempView('total_costs_for_orders')

# COMMAND ----------

spark.sql(
  f'''
          WITH capex AS (
            SELECT 
                omd.order_number
                ,omd.order_type
                ,omd.order_category
                ,omd.created_on
                ,omd.description
                ,omd.processing_group
                ,omd.subtype_name
                ,omd.company_code
                ,omd.plant
                ,omd.phase0_order_created
                ,omd.phase1_order_released
                ,omd.controlling_area
                ,omd.phase2_order_completed
                ,omd.phase3_order_closed
                ,omd.order_currency
                ,omd.object_number
                ,omd.profit_center
                ,bp.ledger
                ,omd.__etl_keys_fprint
                ,omd.__etl_effective_from
                ,omd.__etl_effective_to
                ,omd.__etl_is_active
                ,omd.__etl_is_deleted
                
            FROM
                order_master_data AS omd
            LEFT JOIN 
                budget_planning bp ON 
                bp.currency = omd.order_currency)

                SELECT 
                    c.*
                    ,t1.fiscal_year
                    ,t1.budget_local
                    ,t1.allocated_local
                    ,t2.budget_usd
                    ,cosp.actuals_usd
                    ,cosp.committed_usd
                    ,cosp.actuals_local
                    ,cosp.committed_local
                FROM 
                    capex AS c
                LEFT JOIN 
                    total_records_1 AS t1 ON 
                    t1.object_number = c.object_number AND 
                    t1.ledger = c.ledger
                LEFT JOIN 
                    total_records_2 AS t2 ON 
                    t2.object_number = c.object_number AND 
                    t2.fiscal_year = t1.fiscal_year
                LEFT JOIN 
                    total_costs_for_orders AS cosp ON
                    cosp.object_number = c.object_number AND
                    cosp.fiscal_year = t1.fiscal_year
                '''
                
).createOrReplaceTempView('capex_final')

# COMMAND ----------

spark.sql(
    '''
    WITH ranked_capex AS (
        SELECT 
            cap.*,
            ROW_NUMBER() OVER (
                PARTITION BY cap.order_number, cap.fiscal_year
                ORDER BY cap.__etl_effective_from DESC
            ) AS rn
        FROM capex_final AS cap
    )
    SELECT 
        cap.order_number,
        cap.order_type,
        cap.order_category,
        cap.created_on,
        cap.description,
        cap.processing_group,
        COALESCE(cap.subtype_name, '') AS subtype_name,
        cap.company_code,
        cap.plant,
        cap.phase0_order_created,
        cap.phase1_order_released,
        cap.controlling_area,
        cap.phase2_order_completed,
        cap.phase3_order_closed,
        cap.order_currency,
        cap.object_number,
        cap.profit_center,
        cap.ledger,
        cap.fiscal_year,
        cap.budget_local,
        cap.allocated_local,
        cap.budget_usd,
        cap.actuals_usd,
        cap.committed_usd,
        cap.actuals_local,
        cap.committed_local,
        cap.__etl_keys_fprint,
        cap.__etl_effective_from,
        cap.__etl_effective_to,
        cap.__etl_is_active,
        cap.__etl_is_deleted
    FROM ranked_capex AS cap
    WHERE rn = 1
    '''
).createOrReplaceTempView('capex')


# COMMAND ----------

spark.sql(
    f'''
        select 
            {metadata.get_key_columns_ddl()}
            ,{metadata.get_payload_columns_ddl()}
            ,xxhash64({metadata.get_key_columns_ddl()}) AS __etl_keys_fprint
            ,xxhash64({metadata.get_payload_columns_ddl()}) AS __etl_row_fprint
            ,__etl_effective_from
            ,__etl_effective_to
            ,__etl_is_active
            ,__etl_is_deleted
        from 
            capex
    '''
).createOrReplaceTempView('capex_costs')

# COMMAND ----------

merge_statement = metadata.get_merge_ddl('capex_costs', dest_tablename)
merge_result = spark.sql(merge_statement)
logger.log.info(f'Merge: {dest_tablename} {merge_result.toPandas().head(1)}')