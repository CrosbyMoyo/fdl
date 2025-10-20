# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.exchange_rates_month_on_month_index.yaml"
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

source_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("exchange_rates")}'
dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver")}'

# COMMAND ----------

monthly_exchange_rates = spark.sql(f'''
    SELECT
        * except(er.valid_from, er.scaled_exchange_rate),
        er.scaled_exchange_rate AS exchange_rate,
        er.valid_from           AS effective_date_key
    FROM 
        {source_tablename} AS er
    WHERE 
        exchange_rate_type = 'I'
''')

monthly_exchange_rates.createOrReplaceTempView('monthly_exchange_rates')

# COMMAND ----------

snapshot_months = spark.sql(f'''
    -- For each month get the exchange rate for the previous 12 months in long format
        SELECT
            m0.from_currency,
            m0.to_currency,
            m0.effective_date_key,
            m0.exchange_rate,
            m0.exchange_rate AS lag_exchange_rate,
            0 AS month_lag
        FROM
            monthly_exchange_rates AS m0
    UNION ALL
        SELECT
            m1.from_currency,
            m1.to_currency,
            m1.effective_date_key,
            m1.exchange_rate,
            LAG(m1.exchange_rate, 1) OVER (
                PARTITION BY 
                    m1.from_currency, 
                    m1.to_currency 
                ORDER BY m1.effective_date_key ASC
            ) AS lag_exchange_rate,
            1 AS month_lag
        FROM
            monthly_exchange_rates AS m1
    UNION ALL
        SELECT
            m2.from_currency,
            m2.to_currency,
            m2.effective_date_key,
            m2.exchange_rate,
            LAG(m2.exchange_rate, 2) OVER (
                PARTITION BY 
                    m2.from_currency, 
                    m2.to_currency
                ORDER BY m2.effective_date_key ASC
            ) AS lag_exchange_rate,
            2 AS month_lag
        FROM
            monthly_exchange_rates AS m2
    UNION ALL
        SELECT
            m3.from_currency,
            m3.to_currency,
            m3.effective_date_key,
            m3.exchange_rate,
            LAG(m3.exchange_rate, 3) OVER (
                PARTITION BY 
                    m3.from_currency, 
                    m3.to_currency 
                ORDER BY m3.effective_date_key ASC
            ) AS lag_exchange_rate,
            3 AS month_lag
        FROM
            monthly_exchange_rates AS m3
    UNION ALL
        SELECT
            m4.from_currency,
            m4.to_currency,
            m4.effective_date_key,
            m4.exchange_rate,
            LAG(m4.exchange_rate, 4) OVER (
                PARTITION BY 
                    m4.from_currency,
                    m4.to_currency 
                ORDER BY effective_date_key ASC
            ) AS lag_exchange_rate,
            4 AS month_lag
        FROM
            monthly_exchange_rates AS m4
    UNION ALL
        SELECT
            m5.from_currency,
            m5.to_currency,
            m5.effective_date_key,
            m5.exchange_rate,
            LAG(m5.exchange_rate, 5) OVER (
                PARTITION BY 
                    m5.from_currency,
                    m5.to_currency
                ORDER BY m5.effective_date_key ASC
            ) AS lag_exchange_rate,
            5 AS month_lag
        FROM
            monthly_exchange_rates AS m5
    UNION ALL
        SELECT
            m6.from_currency,
            m6.to_currency,
            m6.effective_date_key,
            m6.exchange_rate,
            LAG(m6.exchange_rate, 6) OVER (
                PARTITION BY 
                    m6.from_currency, 
                    m6.to_currency
                ORDER BY m6.effective_date_key ASC
                ) AS lag_exchange_rate,
            6 AS month_lag
        FROM
            monthly_exchange_rates AS m6
    UNION ALL
        SELECT
            m7.from_currency,
            m7.to_currency,
            m7.effective_date_key,
            m7.exchange_rate,
            LAG(m7.exchange_rate, 7) OVER (
                PARTITION BY 
                    m7.from_currency, 
                    m7.to_currency
                ORDER BY m7.effective_date_key ASC
            ) AS lag_exchange_rate,
            7 AS month_lag
        FROM
            monthly_exchange_rates AS m7
    UNION ALL
        SELECT
            m8.from_currency,
            m8.to_currency,
            m8.effective_date_key,
            m8.exchange_rate,
            LAG(m8.exchange_rate, 8) OVER (
                PARTITION BY 
                    m8.from_currency, 
                    m8.to_currency
                ORDER BY m8.effective_date_key ASC
            ) AS lag_exchange_rate,
            8 AS month_lag
        FROM
            monthly_exchange_rates AS m8
    UNION ALL
        SELECT
            m9.from_currency,
            m9.to_currency,
            m9.effective_date_key,
            m9.exchange_rate,
            LAG(m9.exchange_rate, 9) OVER (
                PARTITION BY 
                    m9.from_currency, 
                    m9.to_currency
                ORDER BY m9.effective_date_key ASC
            ) AS lag_exchange_rate,
            9 AS month_lag
        FROM
            monthly_exchange_rates AS m9
    UNION ALL
        SELECT
            m10.from_currency,
            m10.to_currency,
            m10.effective_date_key,
            m10.exchange_rate,
            LAG(m10.exchange_rate, 10) OVER (
                PARTITION BY 
                    m10.from_currency, 
                    m10.to_currency 
                ORDER BY m10.effective_date_key ASC
            ) AS lag_exchange_rate,
            10 AS month_lag
        FROM
            monthly_exchange_rates AS m10
    UNION ALL
        SELECT
            m11.from_currency,
            m11.to_currency,
            m11.effective_date_key,
            m11.exchange_rate,
            LAG(m11.exchange_rate, 11) OVER (
                PARTITION BY 
                    m11.from_currency, 
                    m11.to_currency
                ORDER BY m11.effective_date_key ASC
            ) AS lag_exchange_rate,
            11 AS month_lag
        FROM
            monthly_exchange_rates AS m11
    UNION ALL
        SELECT
            m12.from_currency,
            m12.to_currency,
            m12.effective_date_key,
            m12.exchange_rate,
            LAG(m12.exchange_rate, 12) OVER (
                PARTITION BY 
                    m12.from_currency,
                    m12.to_currency
                ORDER BY m12.effective_date_key ASC
            ) AS lag_exchange_rate,
            12 AS month_lag
        FROM
            monthly_exchange_rates AS m12
''')

snapshot_months.createOrReplaceTempView('snapshot_months')

# COMMAND ----------

current_snapshot = spark.sql(f'''
    -- Format the above into the 12 month snapshot periods
    -- So, for each and every month, we have the exchange rates for each preceeding month in the last 12 months. This will multiply the row count by 12.
    SELECT 
        sm.effective_date_key AS snapshot_month_key,
        sm.exchange_rate      AS snapshot_exchange_rate,
        sm.from_currency,
        sm.to_currency,
        sm.month_lag,
        date(dateadd(month, -sm.month_lag, sm.effective_date_key)) AS month_key,
        sm.lag_exchange_rate AS exchange_rate
    FROM
        snapshot_months AS sm
''')

current_snapshot.createOrReplaceTempView('current_snapshot')

# COMMAND ----------

comparison = spark.sql(f'''
    -- We want to compare against the first month in a 12 month snapshot period to calculate the month on month rate
    SELECT 
        cs.snapshot_month_key, 
        cs.snapshot_exchange_rate,
        cs.from_currency,
        cs.to_currency,
        cs.month_lag,
        cs.month_key,
        cs.exchange_rate
    FROM 
        current_snapshot AS cs
    WHERE 
        cs.month_lag = 11
''')

comparison.createOrReplaceTempView('comparison')

# COMMAND ----------

enhanced = spark.sql(f'''
    -- Compute the month on month rate
    SELECT 
        s.snapshot_month_key,
        c.month_key                       AS opening_month_key,
        s.month_key,
        s.snapshot_exchange_rate,
        s.from_currency,
        s.to_currency,
        c.exchange_rate                   AS opening_exchange_rate,
        s.exchange_rate,
        s.exchange_rate / c.exchange_rate AS month_on_month_index
    FROM 
        current_snapshot AS s 
        JOIN comparison AS c 
            ON s.snapshot_month_key = c.snapshot_month_key AND 
            s.to_currency = c.to_currency AND
            s.from_currency = c.from_currency
''')

enhanced.createOrReplaceTempView('enhanced')

# COMMAND ----------

write_result = metadata.process_transformation_table('enhanced', env_vars.silver_catalog)
logger.log.info(f'Write: {dest_tablename} {write_result}')