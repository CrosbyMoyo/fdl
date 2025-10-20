# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.exchange_rates.yaml"
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", include_schemaversion=True)}'

# COMMAND ----------

# Front end doesn't allow USD to USD conversions but we have to make sure theyre in here for reporting purposes 
usd_usd_conversions = spark.sql(
    f'''
            SELECT 
                'USD'         AS from_currency
                ,'USD'        AS to_currency
                ,'I'          AS exchange_rate_type
                ,1            AS exchange_rate
                ,'1900-01-01' AS valid_from
        UNION ALL
            SELECT
                'USD'         AS from_currency 
                ,'USD'        AS to_currency
                ,'E'          AS exchange_rate_type
                ,1            AS exchange_rate
                ,'1900-01-01' AS valid_from
        UNION ALL
            SELECT
                'USD'         AS from_currency 
                ,'USD'        AS to_currency
                ,'PA'         AS exchange_rate_type
                ,1            AS exchange_rate
                ,'1900-01-01' AS valid_from
    '''
)

usd_usd_conversions.createOrReplaceTempView('usd_usd_conversions')

# COMMAND ----------

spark.sql(
    f'''
        WITH exchange_rates AS (
                SELECT 
                    r.from_currency
                    ,r.to_currency 
                    ,r.exchange_rate_type
                    ,r.exchange_rate 
                    ,r.valid_from 
                FROM 
                    {env_vars.silver_catalog}.fin_finance_staging.exchange_rates_casted AS r
            UNION ALL 
                SELECT  
                    u.*
                FROM 
                    usd_usd_conversions AS u
        )

        , factors AS (
            SELECT 
                * EXCEPT(f.ratio_from, f.ratio_to)
                ,if(f.ratio_from = 0, 1, f.ratio_from) AS ratio_from
                ,if(f.ratio_to = 0, 1, f.ratio_to)     AS ratio_to
                ,lead(
                    f.valid_from, 1, '9999-12-31'
                ) OVER (
                    PARTITION BY 
                        f.exchange_rate_type, 
                        f.from_currency, 
                        f.to_currency 
                    ORDER BY f.valid_from ASC
                ) AS valid_to
            FROM 
                {env_vars.silver_catalog}.fin_finance.exchange_rate_conversion_factors AS f 
            WHERE 
                f.exchange_rate_type IN ('I', 'PA', 'E')
                AND f.from_currency = 'USD'
        )

        -- At Vivo they only maintain the USD -> LOCAL exchange rates 
        , from_usd AS (
            SELECT
                LEAD(
                    r.valid_from, 1, '9999-12-31'
                ) OVER (
                    PARTITION BY 
                        r.exchange_rate_type, 
                        r.from_currency, 
                        r.to_currency 
                    ORDER BY r.valid_from ASC
                ) AS valid_to,
                *
            FROM
                exchange_rates AS r 
            WHERE 
                r.exchange_rate_type IN ('I', 'PA', 'E')
                AND r.from_currency = 'USD'
        )

        -- Join with exchange rate factors
        -- If the factor doesn't exist we set it to 1
        , with_factors AS (
            SELECT
                r.* 
                ,COALESCE(f.ratio_from, 1)  AS factor_from
                ,COALESCE(f.ratio_to, 1)    AS factor_to
            FROM
                from_usd AS r

            LEFT JOIN factors AS f ON
                r.from_currency = f.from_currency AND
                r.to_currency = f.to_currency AND  
                r.exchange_rate_type = f.exchange_rate_type AND 
                r.valid_from BETWEEN f.valid_from AND f.valid_to
            QUALIFY 
                row_number() OVER (
                    PARTITION BY 
                        r.from_currency, 
                        r.to_currency, 
                        r.exchange_rate_type, 
                        r.valid_from 
                    ORDER BY 
                        f.valid_from DESC
                ) = 1
        )
        
        -- Note the default is 2 
        , with_decimal_places AS (
            SELECT 
                f.*
                ,COALESCE(to_dp.currency_decimal_places, 2)     AS to_decimal_places
            FROM 
                with_factors AS f
                
                LEFT JOIN {env_vars.silver_catalog}.fin_controlling.decimal_places_in_currencies AS to_dp
                    ON f.from_currency = to_dp.currency_key 
        )

        -- Compute the decimal place scaling 
        , with_tdec AS (
            SELECT 
                *
                ,POWER(10, 2 - to_decimal_places)      AS to_tdec 
            FROM 
                with_decimal_places 
        )

        -- Apply the exchange rate scaling
        , scaled_rates AS (
            SELECT 
                *
                ,IF(
                    from_currency = to_currency,
                    exchange_rate * to_tdec,
                    (factor_to * abs(exchange_rate)) / (factor_from * to_tdec)
                ) AS scaled_exchange_rate
            FROM 
                with_tdec
        )

        -- Invert to get the rates to USD. We don't want to inverse the USD - USD conversion
        , to_usd AS (
            SELECT 
                f.* EXCEPT(to_currency, from_currency, scaled_exchange_rate)
                ,f.to_currency                           AS from_currency
                ,f.from_currency                         AS to_currency
                ,(1 / f.scaled_exchange_rate)            AS scaled_exchange_rate
            FROM 
                scaled_rates AS f
                JOIN scaled_rates AS t ON 
                    f.to_currency = t.to_currency AND
                    f.from_currency = t.from_currency AND
                    f.to_currency <> f.from_currency AND 
                    t.to_currency <> f.from_currency AND 
                    f.valid_from = t.valid_from AND 
                    f.exchange_rate_type = t.exchange_rate_type
        )

            SELECT 
                t.from_currency
                ,t.to_currency
                ,t.valid_from
                ,t.valid_to
                ,t.exchange_rate_type
                ,t.scaled_exchange_rate
                ,True AS inverted_rate 
            FROM 
                to_usd AS t 
        UNION ALL 
            SELECT 
                f.from_currency
                ,f.to_currency
                ,f.valid_from
                ,f.valid_to
                ,f.exchange_rate_type
                ,f.scaled_exchange_rate
                ,False AS inverted_rate 
            FROM 
                scaled_rates AS f
    '''
).createOrReplaceTempView('exchange_rates')

# COMMAND ----------

spark.sql(
    f'''
    WITH last_date_of_month AS (
        SELECT DISTINCT 
            d.last_date_of_month 
        FROM 
            {env_vars.gold_catalog}.ca_cross_application_components.dim_date AS d
    )

    , date_keyed AS (
        SELECT
            d.last_date_of_month
            ,r.*
        FROM 
            last_date_of_month AS d  
            LEFT JOIN exchange_rates AS r ON 
                d.last_date_of_month BETWEEN r.valid_from AND r.valid_to
        WHERE 
            r.exchange_rate_type <> 'PA'
        ORDER BY d.last_date_of_month DESC
    )

    SELECT 
        d.last_date_of_month          AS valid_from 
        ,d.from_currency
        ,d.to_currency
        ,d.exchange_rate_type
        ,d.inverted_rate
        ,LAST(d.scaled_exchange_rate) AS scaled_exchange_rate
    FROM
        date_keyed AS d
    GROUP BY 
        d.last_date_of_month 
        ,d.inverted_rate
        ,d.from_currency
        ,d.to_currency
        ,d.exchange_rate_type
    '''
).createOrReplaceTempView('monthly_forward_fill')

# COMMAND ----------

spark.sql(
    f'''
    WITH first_date_of_year AS (
        SELECT DISTINCT 
            d.first_date_of_year 
        FROM 
            {env_vars.gold_catalog}.ca_cross_application_components.dim_date AS d
    )

    , date_keyed AS (
        SELECT
            d.first_date_of_year
            ,r.*
        FROM 
            first_date_of_year AS d
            LEFT JOIN exchange_rates AS r ON 
                d.first_date_of_year BETWEEN r.valid_from AND r.valid_to
        WHERE 
            r.exchange_rate_type = 'PA'
        ORDER BY d.first_date_of_year DESC
    )

    SELECT 
        d.first_date_of_year          AS valid_from 
        ,d.from_currency
        ,d.to_currency
        ,d.exchange_rate_type
        ,d.inverted_rate
        ,LAST(d.scaled_exchange_rate) AS scaled_exchange_rate
    FROM
        date_keyed AS d
    GROUP BY 
        d.first_date_of_year 
        ,d.inverted_rate
        ,d.from_currency
        ,d.to_currency
        ,d.exchange_rate_type
    '''
).createOrReplaceTempView('yearly_forward_fill')

# COMMAND ----------

# Add back on the values that aren't coming through on the correct dates for traceability 
spark.sql(
    f'''
        SELECT 
            r.valid_from 
            ,r.from_currency
            ,r.to_currency
            ,r.exchange_rate_type
            ,r.inverted_rate
            ,r.scaled_exchange_rate
        FROM 
            exchange_rates AS r
        WHERE 
            (r.valid_from <> last_day(r.valid_from) AND r.exchange_rate_type <> 'PA')
            OR (dayofyear(r.valid_from) <> 1 AND r.exchange_rate_type = 'PA')
    '''
).createOrReplaceTempView('shifted_dates')

# COMMAND ----------

spark.sql(
    f'''
        SELECT 
            m.*
        FROM 
            monthly_forward_fill AS m
    UNION ALL 
        SELECT 
            y.*
        FROM 
            yearly_forward_fill AS y 
    UNION ALL 
        SELECT 
            s.* 
        FROM 
            shifted_dates AS s 
    '''
).createOrReplaceTempView('enhanced')

# COMMAND ----------

spark.sql(
    f'''
        SELECT 
            {metadata.get_key_columns_ddl()}
            ,{metadata.get_payload_columns_ddl()}
            ,{metadata.get_key_fprint_ddl()}                AS __etl_keys_fprint
            ,{metadata.get_row_fprint_ddl()}                AS __etl_row_fprint
            ,current_date()                                 AS __etl_effective_from
            ,CAST(NULL AS DATE)                             AS __etl_effective_to
            ,True                                           AS __etl_is_active
            ,False                                          AS __etl_is_deleted
        FROM
            enhanced
    '''
).createOrReplaceTempView('final')

# COMMAND ----------

merge_statement = metadata.get_merge_ddl('final', dest_tablename)
merge_result = spark.sql(merge_statement)
logger.log.info(f'Merge: {dest_tablename} {merge_result.toPandas().head(1)}')