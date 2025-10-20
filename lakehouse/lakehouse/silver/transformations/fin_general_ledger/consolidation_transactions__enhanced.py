# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

dest_metadata_filename = 'silver.finance_transactions_consolidation_historic.yaml'
metadata = MetadataYaml(f'./metadata/{dest_metadata_filename}')
logger.log.info(f'Widget: "metadata_filename" = {dest_metadata_filename}')


# COMMAND ----------

source_manual_load_plan_tablename = f'{env_vars.silver_catalog}.fin_general_ledger_staging.dsp_fi_manual_load_plan__casted'
source_manual_load_plan_2024_tablename = f'{env_vars.silver_catalog}.fin_general_ledger_staging.dsp_fi_manual_load_plan_2024__casted'
source_manual_load_jnls_tablename = f'{env_vars.silver_catalog}.fin_general_ledger_staging.dsp_fi_manual_load_jnls__casted'
source_sac_jnl_tablename = f'{env_vars.silver_catalog}.fin_general_ledger_staging.sac_jnl'
source_convenience_retail_income_stream_mapping_tablename = f'{env_vars.silver_catalog}.fin_general_ledger_staging.convenience_retail_income_stream_mapping'

source_company_code_tablename = f'{env_vars.silver_catalog}.ca_cross_application_components.company_code'
source_cost_center_tablename = f'{env_vars.silver_catalog}.fin_controlling.cost_center'
source_profit_center_tablename = f'{env_vars.silver_catalog}.fin_controlling.profit_center'

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", True)}'

# COMMAND ----------

# MAGIC %md
# MAGIC **READ** GL_Account manual mapping

# COMMAND ----------

convenience_retail_income_stream_mapping = spark.sql(f'''
    SELECT
        s.lab_off,
        s.gl_account,
        s.income_stream,
        s.material_description,
        s.gl_account_description,
        s.default_gl_flag
    FROM
        {source_convenience_retail_income_stream_mapping_tablename} AS s 
''').createOrReplaceTempView('convenience_retail_income_stream_mapping')

# COMMAND ----------

# MAGIC %md
# MAGIC **READ** 2024 Journals (HFM)

# COMMAND ----------

spark.sql(f'''
  SELECT
    --keys
    mlp.gl_account,
    mlp.company_code,
    mlp.intercompany_partner,
    mlp.ud1_volume_flag,
    mlp.ud2_cost_bucket,
    mlp.ud3_budget_holder,
    mlp.ud4_product,
    mlp.ud5_line_of_business,
    mlp.ud6_flexible_field,
    mlp.period,
    mlp.financial_year,
    mlp.journal_type,

    --payload
    cast(round(mlp.amount, 4) AS DECIMAL(23,4)) AS amount
  FROM
    {source_manual_load_jnls_tablename} AS mlp
''').createOrReplaceTempView('mlp_jnls_enhanced')

# COMMAND ----------

# MAGIC %md
# MAGIC **READ** 2025 Manual plan (HFM)

# COMMAND ----------

spark.sql(f'''
  SELECT
    --keys
    mlp.gl_account,
    mlp.company_code,
    mlp.intercompany_partner,
    mlp.ud1_volume_flag,
    mlp.ud2_cost_bucket,
    mlp.ud3_budget_holder,
    mlp.ud4_product,
    mlp.ud5_line_of_business,
    mlp.ud6_flexible_field,
    mlp.period,
    mlp.financial_year,
    mlp.journal_type,

    --payload
    mlp.amount
  FROM
    {source_manual_load_plan_tablename} AS mlp
'''
).createOrReplaceTempView('mlp_enhanced')

# COMMAND ----------

# MAGIC %md
# MAGIC **READ** 2024 Manual Plan

# COMMAND ----------

spark.sql(f'''
  SELECT
    --keys
    mlp.gl_account,
    mlp.company_code,
    mlp.intercompany_partner,
    mlp.ud1_volume_flag,
    mlp.ud2_cost_bucket,
    mlp.ud3_budget_holder,
    mlp.ud4_product,
    mlp.ud5_line_of_business,
    mlp.ud6_flexible_field,
    mlp.period,
    mlp.financial_year,
    mlp.journal_type,

    --payload
    mlp.amount
  FROM
    {source_manual_load_plan_2024_tablename} AS mlp
'''
).createOrReplaceTempView('mlp24_enhanced')

# COMMAND ----------

# MAGIC %md
# MAGIC **READ** SAC Journal

# COMMAND ----------

spark.sql(
  f'''
    SELECT
      --keys
      mlp.gl_account,
      mlp.company_code,
      mlp.intercompany_partner,
      mlp.ud1_volume_flag,
      mlp.ud2_cost_bucket,
      mlp.ud3_budget_holder,
      mlp.ud4_product,
      mlp.ud5_line_of_business,
      mlp.ud6_flexible_field,
      mlp.period,
      mlp.financial_year,
      mlp.journal_type,

      --payload
      mlp.amount
    FROM
      {source_sac_jnl_tablename} AS mlp
  '''
).createOrReplaceTempView('sac_jnl_enhanced')

# COMMAND ----------

# MAGIC %md
# MAGIC **Union** Manual Plans (2024,2025) and SAC Journal

# COMMAND ----------

spark.sql(
  f'''
      SELECT mlp.* FROM mlp_enhanced AS mlp
    UNION
      SELECT mlp24.* FROM mlp24_enhanced AS mlp24
    UNION
      SELECT mlp_jnls.* FROM mlp_jnls_enhanced AS mlp_jnls
    UNION
      SELECT sac.* FROM sac_jnl_enhanced AS sac
  '''
).createOrReplaceTempView('unite_mlp')

# COMMAND ----------

# MAGIC %md
# MAGIC **MAP** GL_Account to manual mapping

# COMMAND ----------

spark.sql(f'''
  SELECT
    c.gl_account AS gl_account_new,
    mlp.gl_account,
    mlp.company_code,
    mlp.intercompany_partner,
    mlp.ud1_volume_flag,
    mlp.ud2_cost_bucket,
    mlp.ud3_budget_holder,
    mlp.ud4_product,
    mlp.ud5_line_of_business,
    mlp.ud6_flexible_field,
    mlp.period,
    mlp.financial_year,
    mlp.amount,
    mlp.journal_type,
    concat(financial_year,date_format(to_date(concat(mlp.period, ' 01'), 'MMM dd'), '0MM')) as fiscal_year_period,
    date_format(to_date(concat(mlp.period, ' 01'), 'MMM dd'), 'MM') as posting_period,
    mlp.financial_year as fiscal_year 
  FROM
    unite_mlp AS mlp
    LEFT JOIN convenience_retail_income_stream_mapping AS c 
        ON c.lab_off = mlp.ud4_product
        AND c.default_gl_flag = true
'''
).createOrReplaceTempView('joined_mlp')

# COMMAND ----------

# MAGIC %md
# MAGIC **READ** company code

# COMMAND ----------

spark.sql(
    f"""
    SELECT
        compc.company_code
        ,compc.currency_key
    FROM
        {source_company_code_tablename} AS compc
"""
).createOrReplaceTempView('company_code')

# COMMAND ----------

# MAGIC %md
# MAGIC **READ** and **AGGREGATE** cost center

# COMMAND ----------

spark.sql(
  f"""
    SELECT
      costc.company_code,
      costc.department,
      MIN(costc.cost_center) AS cost_center
    FROM
      {source_cost_center_tablename} AS costc
    WHERE
      costc.company_code IS NOT NULL 
      AND costc.company_code != ''
      AND costc.department IS NOT NULL 
      AND costc.department != ''
      AND costc.budget_holder IS NOT NULL 
      AND costc.budget_holder != ''
    GROUP BY
      costc.company_code
      ,costc.department
"""
).createOrReplaceTempView('filtered_cost_center')

# COMMAND ----------

# MAGIC %md
# MAGIC **READ** Profit Center to add LOB1 Field
# MAGIC
# MAGIC source_profit_center_tablename

# COMMAND ----------

profit_center_md = spark.sql(
    f"""
        SELECT
            pc.profit_center,
            pc.line_of_business_1,
            pc.volume_flag_ind
        FROM
            {source_profit_center_tablename} AS pc
        WHERE
            pc.valid_to = '9999-12-31' 
"""
)
profit_center_md.createOrReplaceTempView('profit_center_md')

# COMMAND ----------

# MAGIC %md
# MAGIC **MAP** consolidation historic data with business rules (pt.1)

# COMMAND ----------

consolidation_historic_enhanced = spark.sql(
    f"""
        SELECT    
            '0' || substring_index(ch.gl_account, ':', -1) AS gl_account,
            ch.gl_account_new,
            CASE 
                WHEN ch.gl_account LIKE '%:%' THEN REPLACE(substring_index(ch.gl_account, ':', 1), '_', '.')
                WHEN ch.gl_account LIKE '%.%' THEN ch.gl_account 
                ELSE '' 
            END AS vcode,
            CASE 
                WHEN ch.ud1_volume_flag not in ('KG','100000010') THEN CAST((ch.amount * -1) AS DECIMAL(28, 8))
                ELSE CAST((ch.amount * -1000) AS DECIMAL(28, 8))
            END AS amount,
            CASE 
                WHEN ch.ud1_volume_flag not in ('KG','100000010') THEN CAST((ch.amount * -1) AS DECIMAL(28, 8))
                ELSE 0 
            END AS amount_local_currency,
            CASE 
                WHEN ch.ud1_volume_flag IN ('KG','100000010') THEN CAST((ch.amount * -1000) AS DECIMAL(28, 8))
                ELSE 0 
            END AS quantity,
            CASE WHEN ch.intercompany_partner = 'BLANK' THEN '' ELSE ch.intercompany_partner END AS intercompany_partner,
            CASE WHEN ch.ud1_volume_flag = 'BLANK' THEN '' ELSE ch.ud1_volume_flag END AS ud1_volume_flag,
            CASE WHEN ch.ud2_cost_bucket = 'BLANK' THEN '' ELSE ch.ud2_cost_bucket END AS ud2_cost_bucket,
            CASE WHEN ch.ud3_budget_holder = 'BLANK' THEN '' ELSE ch.ud3_budget_holder END AS ud3_budget_holder,
            CASE WHEN ch.ud4_product = 'BLANK' THEN '' ELSE ch.ud4_product END AS ud4_product,
            CASE WHEN ch.ud5_line_of_business = 'BLANK' THEN '' ELSE ch.ud5_line_of_business END AS ud5_line_of_business,
            CASE WHEN ch.ud6_flexible_field = 'BLANK' THEN '' ELSE ch.ud6_flexible_field END AS ud6_flexible_field,
            pc.line_of_business_1 AS line_of_business,
            pc.volume_flag_ind AS volume_flag_ind,
            'OP01' AS chart_of_account,
            'USD' AS group_currency_key,
            'OP01' AS controlling_area,
            from_unixtime(unix_timestamp(ch.period,'MMM'),'MM') AS period,
            ch.financial_year,
            ch.fiscal_year_period,
            ch.posting_period,
            ch.fiscal_year,
            ch.company_code,
            compc.currency_key,
            f.cost_center,
            CASE 
                WHEN ch.journal_type IN ('Specials', 'Retail', 'FTE') THEN 'JNLS'
                WHEN ch.journal_type = 'ICP' THEN 'ELIM'
                WHEN ch.journal_type = 'DR_CR' THEN 'DRCR'
                WHEN ch.journal_type = 'PLAN' THEN 'PLANHFM'
                ELSE ch.journal_type 
            END AS journal_type,
            'HFM' AS datasource,
            CASE
                WHEN ch.ud1_volume_flag = 'KG'  THEN quantity
                ELSE 0
            END AS volume_kg,
            CASE
                WHEN ch.ud1_volume_flag = '100000010' and volume_flag_ind = true  THEN quantity
                ELSE 0
            END AS volume_litres_l20
        FROM
            joined_mlp AS ch
        LEFT JOIN company_code AS compc ON
            ch.company_code = compc.company_code
        LEFT JOIN filtered_cost_center f ON
            ch.company_code = f.company_code
            AND ch.ud3_budget_holder = f.department
        LEFT JOIN profit_center_md pc ON 
            ch.company_code || ch.ud5_line_of_business = pc.profit_center
    """
)

consolidation_historic_enhanced.createOrReplaceTempView('consolidation_historic_enhanced')

# COMMAND ----------

# MAGIC %md
# MAGIC **MAP** consolidation historic data with business rules (pt.2)

# COMMAND ----------

consolidation_historic_final = spark.sql(
    f"""
        SELECT
            ch.gl_account AS gl_account_old,
            CASE 
                WHEN (ch.line_of_business like 'LOB13%' OR ch.line_of_business like 'LOB14%') THEN COALESCE(ch.gl_account_new, ch.gl_account) 
                ELSE ch.gl_account 
            END AS gl_account,
            CASE 
                WHEN ch.vcode = 'P.111.1' THEN 'P.111.111' 
                ELSE ch.vcode 
            END AS vcode,
            LAST_DAY(TO_DATE(CONCAT(ch.financial_year, '-', LPAD(ch.period, 2, '0'), '-01'), 'yyyy-MM-dd')) AS posting_date,
            CASE 
                WHEN ch.journal_type IN ('DRCR', 'ELIM', 'JNLS', 'CCTD') THEN 'Actual'
                WHEN ch.journal_type = 'PLANHFM' THEN 'Plan'
                ELSE ch.journal_type
            END AS actual_plan_code,
            ch.amount_local_currency AS vcode_amount_local,
            ch.amount_local_currency,
            ch.quantity,
            ch.intercompany_partner,
            ch.ud1_volume_flag,
            ch.ud2_cost_bucket,
            ch.ud3_budget_holder,
            ch.ud4_product,
            ch.ud5_line_of_business AS profit_center,
            ch.ud6_flexible_field AS consolidation_record_type,
            ch.line_of_business,
            ch.chart_of_account,
            ch.cost_center,
            ch.currency_key,
            ch.controlling_area,
            ch.financial_year,
            ch.fiscal_year_period,
            ch.posting_period,
            ch.fiscal_year,
            ch.period,
            ch.company_code,
            ch.journal_type,
            ch.volume_kg,
            ch.volume_litres_l20,
            'HFM' AS datasource
        FROM
            consolidation_historic_enhanced AS ch
    """
)

consolidation_historic_final.createOrReplaceTempView('consolidation_historic_final')


# COMMAND ----------

# MAGIC %md
# MAGIC Contra records calculation

# COMMAND ----------

consolidation_historic_final_combined = spark.sql(
    f'''
        SELECT
            chf.actual_plan_code,
            chf.datasource,
            chf.journal_type,
            chf.posting_date,
            chf.gl_account,
            chf.company_code,
            chf.controlling_area,
            chf.currency_key,
            chf.profit_center,
            chf.consolidation_record_type,
            chf.cost_center,
            chf.vcode,
            chf.fiscal_year_period,
            chf.posting_period,
            chf.fiscal_year,
            CAST(chf.vcode_amount_local AS DECIMAL(38,16)) AS vcode_amount_local,
            chf.amount_local_currency,
            chf.quantity,
            chf.volume_kg,
            chf.volume_litres_l20
            
        FROM
            consolidation_historic_final AS chf

        UNION ALL
        SELECT
            chfi.actual_plan_code,
            chfi.datasource,
            chfi.journal_type,
            last_day(chfi.posting_date  + INTERVAL 1 MONTH) AS posting_date,
            chfi.gl_account,
            chfi.company_code,
            chfi.controlling_area,
            chfi.currency_key,
            chfi.profit_center,
            chfi.consolidation_record_type,
            chfi.cost_center,
            chfi.vcode,
            chfi.fiscal_year_period,
            chfi.period,
            chfi.fiscal_year,
            CAST((chfi.vcode_amount_local * -1) AS DECIMAL(38,16))  AS vcode_amount_local,
            chfi.amount_local_currency * -1                         AS amount_local_currency,
            chfi.quantity * -1                                      AS quantity,
            chfi.volume_kg * -1                                     AS volume_kg,
            chfi.volume_litres_l20 * -1                             AS volume_litres_l20
        FROM
            consolidation_historic_final AS chfi
        WHERE month(chfi.posting_date) <> 12
    '''
) 

consolidation_historic_final_combined.createOrReplaceTempView('consolidation_historic_final_combined')

# COMMAND ----------

# All measures from HFM are YTD so we take the previous month YTD value and subtract to determine the movement
consolidation_historic_final_extended = spark.sql(
    f"""
        SELECT
            chfc.actual_plan_code,
            chfc.datasource,
            chfc.journal_type,
            chfc.posting_date,
            chfc.gl_account,
            chfc.company_code,
            chfc.controlling_area,
            chfc.currency_key,
            chfc.profit_center,
            chfc.consolidation_record_type,
            chfc.cost_center,
            chfc.vcode,
            chfc.fiscal_year_period,
            chfc.posting_period,
            chfc.fiscal_year,
            cast(sum(chfc.vcode_amount_local) AS DECIMAL(38,16)) AS vcode_amount_local,
            sum(chfc.amount_local_currency)                      AS amount_local_currency,
            sum(chfc.quantity)                                   AS quantity,   
            sum(chfc.volume_kg)                                  AS volume_kg,
            sum(chfc.volume_litres_l20)                          AS volume_litres_l20
        FROM
            consolidation_historic_final_combined AS chfc
        GROUP BY  
            chfc.actual_plan_code,
            chfc.datasource,
            chfc.journal_type,
            chfc.posting_date,
            chfc.gl_account,
            chfc.company_code,
            chfc.controlling_area,
            chfc.currency_key,
            chfc.profit_center,
            chfc.consolidation_record_type,
            chfc.cost_center,
            chfc.vcode,
            chfc.fiscal_year_period,
            chfc.posting_period,
            chfc.fiscal_year
    """
)

consolidation_historic_final_extended.createOrReplaceTempView(
    "consolidation_historic_final_extended"
)

# COMMAND ----------

spark.sql(
    f'''
        SELECT 
            {metadata.get_key_columns_ddl()}
            ,{metadata.get_payload_columns_ddl()}
            ,xxhash64({metadata.get_key_columns_ddl()})     AS __etl_keys_fprint
            ,xxhash64({metadata.get_payload_columns_ddl()}) AS __etl_row_fprint
            ,current_date()                                 AS __etl_effective_from
            ,CAST(NULL AS DATE)                             AS __etl_effective_to
            ,True                                           AS __etl_is_active
            ,False                                          AS __etl_is_deleted
        FROM
            consolidation_historic_final_extended
    '''
).createOrReplaceTempView('final')

# COMMAND ----------

write_result = spark.sql(f'''
  INSERT OVERWRITE TABLE {dest_tablename}
  SELECT 
    f.actual_plan_code,
    f.datasource,
    f.journal_type,
    f.posting_date,
    f.gl_account,
    f.company_code,
    f.controlling_area,
    f.currency_key,
    f.profit_center,
    f.consolidation_record_type,
    f.cost_center,
    f.vcode,
    f.fiscal_year_period,
    f.posting_period,
    f.fiscal_year,
    f.vcode_amount_local,
    f.amount_local_currency,
    f.quantity,
    f.volume_kg,
    f.volume_litres_l20,
    f.__etl_keys_fprint,
    f.__etl_row_fprint,
    f.__etl_effective_from,
    f.__etl_effective_to,
    f.__etl_is_active,
    f.__etl_is_deleted
    FROM 
      final AS f 
''')

logger.log.info(f'Merge: {dest_tablename} {write_result.toPandas().head(1)}')

# COMMAND ----------

logger.log.info(f'Merge: {dest_tablename} {write_result.toPandas().head(1)}')