# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

from datetime import datetime, timezone

# COMMAND ----------

dbutils.widgets.text(
    name='metadata_filename',
    defaultValue='gold.dim.date.yaml',
    label='1 - metadata_filename'
)

# COMMAND ----------

metadata_filename = dbutils.widgets.get('metadata_filename')
logger.log.info(f'Widget: metadata_filename = "{metadata_filename}"')

# COMMAND ----------

metadata = GoldMetadataYaml(
    file_path = f'./metadata/{metadata_filename}',
    slv_catalog = env_vars.silver_catalog,
    gld_catalog = env_vars.gold_catalog
)

# COMMAND ----------

load_timestamp = datetime.now(timezone.utc)

# COMMAND ----------

d_tablename = metadata.source_3partname(
    tablename='sap_dates',
    include_schemaversion=True
)

# COMMAND ----------

# dim_date is SCD 0 because it will never change, hence INSERT OVERWRITE

gold_table = spark.sql(f'''
    WITH date_calcs AS (

        SELECT
            d.calendar_date AS date_key
            ,cast(date_format(d.calendar_date, 'yyyyMMdd') AS INT) AS date_number

            -- date parts
            ,d.calendar_year AS cal_year
            ,d.half_year AS cal_half
            ,d.calendar_quarter AS cal_quarter
            ,d.calendar_month AS cal_month
            ,d.calendar_day AS cal_day

            ,d.calendar_day_of_year AS day_of_year
            ,d.week_day AS day_of_week
            ,d.calendar_week AS weeknum

            --strings
            ,date_format(d.calendar_date, 'yyyy-MM-dd') AS date_ISO
            ,date_format(d.calendar_date, 'dd-MMM-yyyy') AS date_short
            ,date_format(d.calendar_date, 'dd MMMM yyyy') AS date_long

            ,concat(d.calendar_year, '-H', d.half_year) AS year_half
            ,concat(d.calendar_year, '-Q', d.calendar_quarter) AS year_quarter
            ,date_format(d.calendar_date, 'yyyy-MM') AS year_month
            ,date_format(d.calendar_date, 'yyyy-MMM') AS year_month_short

            ,concat('H', d.half_year) AS half_name
            ,concat('Q', d.calendar_quarter) AS quarter_name
            ,date_format(d.calendar_date, 'MMM') AS month_short_name
            ,date_format(d.calendar_date, 'MMMM') AS month_full_name
            ,date_format(d.calendar_date, 'MM') AS month_padded
            ,date_format(d.calendar_date, 'E') AS day_short_name
            ,date_format(d.calendar_date, 'EEEE') AS day_full_name
            ,date_format(d.calendar_date, 'dd') AS day_padded
            ,concat(
                d.calendar_day,
                CASE
                WHEN d.calendar_day IN (1, 21, 31) THEN 'st'
                WHEN d.calendar_day IN (2, 22) THEN 'nd'
                WHEN d.calendar_day IN (3, 23) THEN 'rd'
                ELSE 'th'
                END
            ) AS day_ordinal

            -- SAP idiosyncratic formats
            ,d.year_quarter AS sap_year_quarter
            ,d.year_month AS sap_year_month
            ,d.year_week AS sap_year_week
            ,d.year_day AS sap_year_day

            -- date demarcations
            ,min(d.calendar_date) OVER (PARTITION BY d.calendar_year) AS first_date_of_year
            ,max(d.calendar_date) OVER (PARTITION BY d.calendar_year) AS last_date_of_year
            ,min(d.calendar_date) OVER (PARTITION BY d.calendar_year, d.half_year) AS first_date_of_half
            ,max(d.calendar_date) OVER (PARTITION BY d.calendar_year, d.half_year) AS last_date_of_half
            ,min(d.calendar_date) OVER (PARTITION BY d.calendar_year, d.calendar_quarter) AS first_date_of_quarter
            ,max(d.calendar_date) OVER (PARTITION BY d.calendar_year, d.calendar_quarter) AS last_date_of_quarter
            ,d.first_day_of_month AS first_date_of_month
            ,d.last_day_of_month AS last_date_of_month

            -- booleans
            ,CASE
                WHEN d.week_day < 6 THEN True
                ELSE False
            END AS is_weekday
            ,NOT is_weekday AS is_weekend
            ,CASE
                WHEN (d.calendar_year % 4 = 0 AND d.calendar_year % 100 != 0)
                    OR (d.calendar_year % 400 = 0)
                THEN True
                ELSE False
            END AS is_leap_year
            ,CASE
                WHEN d.calendar_month = 2 AND d.calendar_day = 29
                    THEN TRUE
                ELSE False
            END AS is_leap_day

        FROM
            {d_tablename} AS d
        ORDER BY
            d.calendar_date

    ),
    date_metadata AS (
        SELECT
            dc.*
            ,xxhash64(
                dc.date_key
            ) AS __etl_keys_fprint
            ,xxhash64(
                dc.* EXCEPT (dc.date_key)
            ) AS __etl_row_fprint
            ,'{load_timestamp}' AS __etl_effective_from
            ,NULL AS __etl_effective_to
            ,TRUE AS __etl_is_active
            ,FALSE AS __etl_is_deleted
        FROM
            date_calcs AS dc
    )
    INSERT OVERWRITE TABLE {metadata.dest_3partname(include_schemaversion=True)}
        (
            date_key
            ,date_number
            ,cal_year
            ,cal_half
            ,cal_quarter
            ,cal_month
            ,cal_day
            ,day_of_year
            ,day_of_week
            ,weeknum

            -- strings
            ,date_ISO
            ,date_short
            ,date_long
            ,year_half
            ,year_quarter
            ,year_month
            ,year_month_short
            ,half_name
            ,quarter_name
            ,month_short_name
            ,month_full_name
            ,month_padded
            ,day_short_name
            ,day_full_name
            ,day_padded
            ,day_ordinal

            -- SAP idiosyncratic formats
            ,sap_year_quarter
            ,sap_year_month
            ,sap_year_week
            ,sap_year_day

            -- date demarcations
            ,first_date_of_year
            ,last_date_of_year
            ,first_date_of_half
            ,last_date_of_half
            ,first_date_of_quarter
            ,last_date_of_quarter
            ,first_date_of_month
            ,last_date_of_month

            -- booleans
            ,is_weekday
            ,is_weekend
            ,is_leap_year
            ,is_leap_day

        )
    SELECT
        d.date_key
        ,d.date_number
        ,d.cal_year
        ,d.cal_half
        ,d.cal_quarter
        ,d.cal_month
        ,d.cal_day
        ,d.day_of_year
        ,d.day_of_week
        ,d.weeknum

        -- strings
        ,d.date_ISO
        ,d.date_short
        ,d.date_long
        ,d.year_half
        ,d.year_quarter
        ,d.year_month
        ,d.year_month_short
        ,d.half_name
        ,d.quarter_name
        ,d.month_short_name
        ,d.month_full_name
        ,d.month_padded
        ,d.day_short_name
        ,d.day_full_name
        ,d.day_padded
        ,d.day_ordinal

        -- SAP idiosyncratic formats
        ,d.sap_year_quarter
        ,d.sap_year_month
        ,d.sap_year_week
        ,d.sap_year_day

        -- date demarcations
        ,d.first_date_of_year
        ,d.last_date_of_year
        ,d.first_date_of_half
        ,d.last_date_of_half
        ,d.first_date_of_quarter
        ,d.last_date_of_quarter
        ,d.first_date_of_month
        ,d.last_date_of_month

        -- booleans
        ,d.is_weekday
        ,d.is_weekend
        ,d.is_leap_year
        ,d.is_leap_day

    FROM
        date_metadata AS d;
''')


# COMMAND ----------

display(gold_table)