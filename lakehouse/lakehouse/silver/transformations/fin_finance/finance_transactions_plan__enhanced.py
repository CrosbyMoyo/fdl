# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.finance_transactions_plan.yaml"
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", include_schemaversion=True)}'

# COMMAND ----------

collection_segments = spark.sql(f'''
    SELECT 
        *,
        left(collection_segment, 4) AS Company_Code
    FROM 
        {env_vars.silver_catalog}.fin_finance.collection_segments_in_business_partner              
''')

collection_segments.createOrReplaceTempView('collection_segments')

# COMMAND ----------

plan_transactions = spark.sql(f'''
        SELECT 
            plan_23.version
            ,plan_23.plan_period
            ,plan_23.gl_account
            ,plan_23.company_code
            ,plan_23.profit_center
            ,plan_23.customer
            ,plan_23.cost_center
            ,plan_23.material
            ,plan_23.vcode
            ,plan_23.signed_data
        FROM 
            {env_vars.silver_catalog}.fin_finance_staging.finance_transactions_plan_sac_2023__casted AS plan_23
    UNION 
        SELECT 
            plan_24_25.version
            ,plan_24_25.plan_period
            ,plan_24_25.gl_account
            ,plan_24_25.company_code
            ,plan_24_25.profit_center
            ,plan_24_25.customer
            ,plan_24_25.cost_center
            ,plan_24_25.material
            ,plan_24_25.vcode
            ,plan_24_25.signed_data
        FROM 
            {env_vars.silver_catalog}.fin_finance_staging.finance_transactions_plan_sac_2024_2025__casted AS plan_24_25
'''
)

plan_transactions.createOrReplaceTempView('plan_transactions')

# COMMAND ----------

plan_transactions_filtered = spark.sql(f'''
    SELECT 
        last_day(to_date(t.plan_period || '01', 'yyyyMMdd')) AS date_key
        ,CAST(substring(version, 13, 4) AS INT) AS plan_year
        ,t.* EXCEPT(t.plan_period)
    FROM
        plan_transactions AS t
    WHERE 
        t.signed_data <> 0
''')

plan_transactions_filtered.createOrReplaceTempView('plan_transactions_filtered')

# COMMAND ----------

transformed = spark.sql(f'''
    SELECT
        t.* except(t.gl_account, t.customer, t.cost_center, t.version, t.vcode)
        ,REPLACE(t.version, 'public.', '') AS plan_version
        ,IF(
            t.gl_account = 'PSA002',
            '0100000010',
            '0' || substr(t.gl_account, charindex('/', t.gl_account) + 1, len(t.gl_account))
        ) AS gl_account
        ,IF(
            t.customer = '#',
            '',
            '000' || substr(t.customer, charindex('/', t.customer) + 1, len(t.customer))
        ) AS customer
        ,IF(
            t.cost_center = '#',
            '',
            '0' || substr(t.cost_center, charindex('/', t.cost_center) + 1, len(t.cost_center))
        ) AS cost_center
        ,IF(
            t.profit_center = '#',
            '',
            LEFT(t.profit_center, 4)
        ) AS controlling_area
        -- We have a valid entry of '#' in the mapping table for LOB
        ,IF(
            t.profit_center = '#',
            '#',
            substr(t.profit_center, 5, len(t.profit_center))
        ) AS line_of_business
        ,CASE 
            WHEN t.plan_year >= 2024 AND t.vcode = 'V.001' THEN 'P.111.111'
            WHEN t.gl_account = 'PSA002' AND t.customer <> '#' THEN 'P.111.111'
            WHEN t.vcode = 'P.111.1' THEN 'P.111.111'
            ELSE t.vcode
        END AS vcode
        ,CASE 
            WHEN t.plan_year >= 2024 AND t.gl_account = 'PSA002' IS TRUE THEN 0 
            ELSE t.signed_data * -1 
        END AS amount_local_currency
        ,CASE 
            WHEN t.plan_year >= 2024 AND t.gl_account = 'PSA002' IS TRUE THEN t.signed_data
            ELSE 0
        END AS quantity 

        -- Debug 
        ,t.vcode AS _src_vcode
        ,IF(t.gl_account = 'PSA002', True, False) AS _psa_indicator
    FROM 
        plan_transactions_filtered AS t
''')

transformed.createOrReplaceTempView('transformed')

# COMMAND ----------

joined = spark.sql(f'''
    SELECT
        -- Debug 
        t._psa_indicator
        ,t._src_vcode
        
        -- Keys 
        ,t.plan_year
        ,t.plan_version
        ,t.date_key
        ,t.material
        ,t.vcode
        ,t.company_code
        ,t.gl_account
        ,t.customer
        ,t.cost_center
        ,IF(
            t.line_of_business = '#',
            '',
            t.line_of_business
        ) AS line_of_business
        ,IF(
            pc.profit_center IS NOT NULL,
            pc.profit_center,
            ''
        ) AS profit_center
        ,t.controlling_area

        -- Measures
        ,cc.currency_key
        ,cs.collection_segment
        ,cs.collection_specialist
        ,pc.volume_flag_ind
        ,t.amount_local_currency
        ,t.quantity
        ,t.signed_data
    FROM 
        transformed AS t
        
        LEFT JOIN collection_segments AS cs 
            ON t.company_code = cs.company_code 
            AND t.customer = cs.business_partner
            AND t.date_key BETWEEN cs.Created_On AND cs.Closed_On

        LEFT JOIN {env_vars.silver_catalog}.fin_controlling.line_of_business_profit_center_mapping AS pcm 
            ON t.line_of_business = pcm.line_of_business

        LEFT JOIN {env_vars.silver_catalog}.fin_controlling.profit_center AS pc 
            ON t.company_code || pcm.profit_center || '01' = pc.profit_center

        LEFT JOIN {env_vars.silver_catalog}.ca_cross_application_components.company_code AS cc 
            ON t.company_code = cc.company_code
    WHERE 
        t.vcode <> '#'
''')

joined.createOrReplaceTempView('joined')

# COMMAND ----------

generated_vcodes = spark.sql(f'''
    -- Generate a V.001 line for every non zero quantity 
        SELECT 
            v.* except(v.vcode)
            ,'V.001' AS vcode
            ,True AS _generated_flag
        FROM 
            joined AS v
    UNION 
        SELECT 
            r.* except(r.vcode)
            ,r.vcode
            ,False AS _generated_flag
        FROM 
            joined AS r
''')

generated_vcodes.createOrReplaceTempView('generated_vcodes')

# COMMAND ----------

volume_vcode = spark.sql(f'''
    -- Original volume lines from source (vcode = V.001)
    SELECT 
        -- Debug 
        g._generated_flag
        ,g._psa_indicator

        ,g.plan_year
        ,g.plan_version
        ,g.date_key
        ,g.material
        ,g.vcode
        ,g.company_code
        ,g.gl_account
        ,g.customer
        ,g.cost_center
        ,g.line_of_business
        ,g.profit_center
        ,g.currency_key
        ,g.collection_segment
        ,g.collection_specialist
        ,g.controlling_area
        ,g.volume_flag_ind
        
        ,g.quantity AS vcode_amount_local
        ,0          AS quantity
        ,0          AS amount_local_currency
        ,g.signed_data
    FROM
        generated_vcodes AS g
    WHERE 
        g.vcode IN ('V.001')
''')

volume_vcode.createOrReplaceTempView('volume_vcode')

# COMMAND ----------

revenue_vcode = spark.sql(f'''
    -- Revenue lines from source 
    SELECT 
        -- Debug 
        g._generated_flag
        ,g._psa_indicator

        ,g.plan_year
        ,g.plan_version
        ,g.date_key
        ,g.material
        ,g.vcode
        ,g.company_code
        ,g.gl_account
        ,g.customer
        ,g.cost_center
        ,g.line_of_business
        ,g.profit_center
        ,g.currency_key
        ,g.collection_segment
        ,g.collection_specialist
        ,g.controlling_area
        ,g.volume_flag_ind

        ,g.amount_local_currency AS vcode_amount_local
        ,0 AS quantity
        ,g.amount_local_currency AS amount_local_currency
        ,g.signed_data
    FROM
        generated_vcodes AS g
    WHERE 
        g.vcode IN ('P.111.111')
''')

revenue_vcode.createOrReplaceTempView('revenue_vcode')

# COMMAND ----------

reassigned_revenue_vcode = spark.sql(f'''
    SELECT 
        -- Debug 
        g._generated_flag
        ,g._psa_indicator

        ,g.plan_year
        ,g.plan_version
        ,g.date_key
        ,g.material
        ,'P.111.111' AS vcode
        ,g.company_code
        ,g.gl_account
        ,g.customer
        ,g.cost_center
        ,g.line_of_business
        ,g.profit_center
        ,g.currency_key
        ,g.collection_segment
        ,g.collection_specialist
        ,g.controlling_area
        ,g.volume_flag_ind

        ,0 AS vcode_amount_local
        ,g.quantity AS quantity
        ,0 AS amount_local_currency
        ,g.signed_data

    FROM
        generated_vcodes AS g
    WHERE 
        g.vcode IN ('V.001')
''')

reassigned_revenue_vcode.createOrReplaceTempView('reassigned_revenue_vcode')

# COMMAND ----------

other_vcodes = spark.sql(f'''
    -- Everything else 
    SELECT 
        -- Debug 
        g._generated_flag
        ,g._psa_indicator

        ,g.plan_year
        ,g.plan_version
        ,g.date_key
        ,g.material
        ,g.vcode
        ,g.company_code
        ,g.gl_account
        ,g.customer
        ,g.cost_center
        ,g.line_of_business
        ,g.profit_center
        ,g.currency_key
        ,g.collection_segment
        ,g.collection_specialist
        ,g.controlling_area
        ,g.volume_flag_ind
        
        ,g.amount_local_currency AS vcode_amount_local
        ,g.quantity
        ,g.amount_local_currency
        ,g.signed_data
    FROM
        generated_vcodes AS g
    WHERE 
        g.vcode NOT IN ('V.001', 'P.111.111')
''')

other_vcodes.createOrReplaceTempView('other_vcodes')

# COMMAND ----------

combined_vcodes = spark.sql(f'''
        SELECT o.*, 'other' AS _vcode_indicator FROM other_vcodes AS o
    UNION 
        SELECT v.*, 'volume' AS _vcode_indicator FROM volume_vcode AS v
    UNION
        SELECT rr.*, 'reassigned_revenue' AS _vcode_indicator FROM reassigned_revenue_vcode AS rr
    UNION 
        SELECT r.*, 'revenue' AS _vcode_indicator FROM revenue_vcode AS r
''')

combined_vcodes.createOrReplaceTempView('combined_vcodes')

# COMMAND ----------

volume_calc = spark.sql(f'''
    SELECT 
        c.*
        ,CASE
            WHEN vcode IN ('V.001', 'V.002') THEN 0 
            WHEN c.gl_account IN ('0100000010', '0100000021') AND c.volume_flag_ind = True THEN c.quantity
            ELSE 0
        END AS volume_litres_l20
        ,0  AS volume_kg -- We don't have product sold group so can't identify this 
        ,CASE
            WHEN vcode IN ('V.001', 'V.002') THEN 0 
            WHEN c.gl_account = '0120100010' AND c.volume_flag_ind = True THEN c.quantity
            ELSE 0
        END AS volume_issued_litres_l20
    FROM 
        combined_vcodes AS c 
''')

volume_calc.createOrReplaceTempView('volume_calc')

# COMMAND ----------

enhanced = spark.sql(f'''
    SELECT 
        c.plan_year
        ,c.plan_version
        ,c.date_key
        ,c.material
        ,c.vcode
        ,c.company_code
        ,c.gl_account
        ,c.customer
        ,c.cost_center
        ,c.line_of_business
        ,c.profit_center
        ,c.currency_key
        ,c.collection_segment
        ,c.collection_specialist
        ,c.controlling_area
        ,c.volume_flag_ind
        ,SUM(c.vcode_amount_local)          AS vcode_amount_local 
        ,SUM(c.quantity)                    AS quantity
        ,SUM(c.volume_litres_l20)           AS volume_litres_l20
        ,SUM(c.volume_kg)                   AS volume_kg
        ,SUM(c.volume_issued_litres_l20)    AS volume_issued_litres_l20
        ,SUM(c.amount_local_currency)       AS amount_local_currency  
    FROM 
        volume_calc AS c
    GROUP BY ALL
''')

enhanced.createOrReplaceTempView('enhanced')

# COMMAND ----------

final = spark.sql(f'''
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
        enhanced
''')

final.createOrReplaceTempView('final')

# COMMAND ----------

merge_statement = metadata.get_merge_ddl('final', dest_tablename)
merge_result = spark.sql(merge_statement)
logger.log.info(f'Merge: {dest_tablename} {merge_result.toPandas().head(1)}')