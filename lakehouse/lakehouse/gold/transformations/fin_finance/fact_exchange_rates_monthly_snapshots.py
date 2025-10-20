# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

from datetime import datetime, timezone

load_timestamp = datetime.now(timezone.utc)

# COMMAND ----------

dbutils.widgets.text(
    name='metadata_filename',
    defaultValue='gold.fact.fact_exchange_rates_monthly_snapshots.yaml',
    label='1 - metadata_filename'
)

# COMMAND ----------

metadata_filename = dbutils.widgets.get('metadata_filename')
assert metadata_filename, 'metadata_filename must be provided'

logger.log.info(f'Widget: metadata_filename = "{metadata_filename}"')

# COMMAND ----------

metadata = GoldMetadataYaml(
    file_path=f'./metadata/{metadata_filename}',
    slv_catalog=env_vars.silver_catalog,
    gld_catalog=env_vars.gold_catalog
)

# COMMAND ----------

destination = metadata.dest_3partname(True)

# COMMAND ----------

profit_centre_md = spark.sql(f'''
    SELECT
        profit_center_md.*
    FROM
        {env_vars.gold_catalog}.fin_controlling.dim_profit_center AS profit_center_md
    WHERE
        NOT profit_center_md.profit_center LIKE '%SUFU%'
        AND NOT profit_center_md.profit_center LIKE '%DIFU%'
        AND NOT profit_center_md.profit_center LIKE '%PULU%'
        AND NOT profit_center_md.profit_center LIKE '%SULU%'
        AND NOT profit_center_md.profit_center LIKE '%PULG%'
''')
 
profit_centre_md.createOrReplaceTempView('profit_centre_md')

# COMMAND ----------

vcodes_md = spark.sql(f'''
    SELECT
        vcode_md.*
    FROM
        {env_vars.gold_catalog}.fin_general_ledger.dim_vcodes AS vcode_md
    WHERE
        vcode_md.vcode = 'P.111.111'
''')
 
vcodes_md.createOrReplaceTempView('vcodes_md')

# COMMAND ----------

company_md = spark.sql(f'''
    SELECT 
        c.*
    FROM 
        {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code AS c
    WHERE
        c.entity_grouping_level_top = 'Vivo Energy'
        AND c.country_key NOT IN ('NL')
        AND NOT (c.country_key = 'MU' AND c.currency_skey IN ('USD', 'EUR'))
        AND NOT (c.country_key = 'GB' AND c.currency_skey IN ('USD', 'EUR')) 
        AND NOT (c.country_key = 'KE' AND c.currency_skey IN ('USD', 'EUR')) 
''')
 
company_md.createOrReplaceTempView('company_md')

# COMMAND ----------

monthly_volumes = spark.sql(f'''
    SELECT
        LAST_DAY(t.date_key)      AS month_key
        ,SUM(t.volume_litres_l20) AS monthly_volume_litres_l20
        ,c.country_key
    FROM 
        {env_vars.gold_catalog}.fin_finance.fact_finance_transaction_details AS t
        LEFT JOIN vcodes_md AS v ON 
            t.vcode_skey = v.vcode_skey
        JOIN company_md AS c ON 
            t.company_code_skey = c.company_code_skey
        LEFT JOIN profit_centre_md AS p ON 
            p.profit_center_skey = t.profit_center_skey
    WHERE
        t.actual_plan_code = 'Actual'
    GROUP BY
        LAST_DAY(t.date_key),
        c.country_key
''')
 
monthly_volumes.createOrReplaceTempView('monthly_volumes')

# COMMAND ----------

summary_monthly_volumes = spark.sql(f'''
    SELECT 
        mv.month_key,
        mv.country_key, 
        SUM(mv.monthly_volume_litres_l20) OVER (
            PARTITION BY mv.country_key 
            ORDER BY mv.month_key ASC 
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS total_volume_litres_l20_per_country
    FROM 
        monthly_volumes AS mv                         
''')

summary_monthly_volumes.createOrReplaceTempView('summary_monthly_volumes')

# COMMAND ----------

rolling_monthly_volumes = spark.sql(f'''
    SELECT
        smv.*
        ,SUM(smv.total_volume_litres_l20_per_country) OVER (
            PARTITION BY smv.month_key 
            ORDER BY smv.month_key ASC
        ) AS total_volume_litres_l20
    FROM summary_monthly_volumes AS smv
''')
 
rolling_monthly_volumes.createOrReplaceTempView('rolling_monthly_volumes')

# COMMAND ----------

total_volume_with_weights = spark.sql(f'''
    SELECT 
        rmv.month_key,
        rmv.country_key,
        rmv.total_volume_litres_l20,
        rmv.total_volume_litres_l20_per_country,
        if(
            try_divide(rmv.total_volume_litres_l20_per_country, rmv.total_volume_litres_l20) IS NULL, 
            0, 
            try_divide(rmv.total_volume_litres_l20_per_country, rmv.total_volume_litres_l20)
        ) AS weighting
    FROM 
        rolling_monthly_volumes AS rmv
''')
 
total_volume_with_weights.createOrReplaceTempView('total_volume_with_weights')

# COMMAND ----------

country_md = spark.sql(f'''
    SELECT DISTINCT 
        cm.country_key
        ,cm.currency_skey
    FROM 
        company_md AS cm
''')
 
country_md.createOrReplaceTempView('country_md')

# COMMAND ----------

mom_with_countries = spark.sql(f'''
    SELECT 
        mom.*,
        from_country.country_key AS from_country_key
    FROM
        {env_vars.silver_catalog}.fin_finance.exchange_rates_month_on_month_index AS mom 
        JOIN country_md AS from_country
            ON mom.from_currency = from_country.currency_skey
    WHERE 
        mom.to_currency = 'USD'
        AND mom.from_currency <> 'USD'
''')
 
mom_with_countries.createOrReplaceTempView('mom_with_countries')

# COMMAND ----------

mom_weighted = spark.sql(f'''
    SELECT 
        mom.* except (mom.from_currency, mom.to_currency),
        transaction_volumes.total_volume_litres_l20_per_country    AS total_volume_litres_l20,
        transaction_volumes.weighting,
        (mom.month_on_month_index * transaction_volumes.weighting) AS month_on_month_index_weighted
    FROM
        mom_with_countries AS mom 
    JOIN total_volume_with_weights AS transaction_volumes 
        ON mom.snapshot_month_key = transaction_volumes.month_key AND 
        mom.from_country_key = transaction_volumes.country_key
''')
 
mom_weighted.createOrReplaceTempView('mom_weighted')

# COMMAND ----------

opening_month_values = spark.sql(f'''
    SELECT 
        mw.* 
    FROM 
        mom_weighted AS mw
    WHERE mw.opening_month_key = mw.month_key
''')
 
opening_month_values.createOrReplaceTempView('opening_month_values')

# COMMAND ----------

mom_movements = spark.sql(f'''
    SELECT 
        mom.*
        ,open_mth.month_on_month_index_weighted AS opening_month_on_month_index_weighted
        ,mom.month_on_month_index_weighted - opening_month_on_month_index_weighted AS month_on_month_movement 
    FROM 
        mom_weighted AS mom
    LEFT JOIN opening_month_values AS open_mth
        ON mom.opening_month_key = open_mth.month_key
        AND mom.from_country_key = open_mth.from_country_key
''')
 
mom_movements.createOrReplaceTempView('mom_movements')

# COMMAND ----------

gold_table = spark.sql(f'''
    SELECT
        -- Foreign Keys 
        m.from_country_key,
        m.snapshot_month_key,
        m.opening_month_key,
        m.month_key,

        -- Payload
        CASE 
            WHEN m.month_key = m.opening_month_key THEN True
            ELSE False 
        END AS opening_month_flag,
        m.month_on_month_index,
        m.month_on_month_index_weighted,
        m.opening_month_on_month_index_weighted,
        m.month_on_month_movement
    FROM 
        mom_movements AS m
''')

gold_table.createOrReplaceTempView('gold_table')

# COMMAND ----------

hashed_gold_table = spark.sql(f'''{metadata.get_etl_fields_ddl('gold_table')}''')

hashed_gold_table.createOrReplaceTempView('hashed_gold_table')

# COMMAND ----------

metadata.insert_overwrite('hashed_gold_table', destination)