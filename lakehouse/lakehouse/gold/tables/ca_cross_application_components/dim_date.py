# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.gold_catalog}.ca_cross_application_components.dim_date
    (
        date_key DATE
            COMMENT "Date as a DATE datatype.  PK for the table."
            CONSTRAINT dim_date_pk PRIMARY KEY,

        -- numbers
        date_number INT
            COMMENT "Date as an integer - yyyyMMdd",
        cal_year INT
            COMMENT "Year as an integer",
        cal_half INT
            COMMENT "The half of the year (Jan-Jun) as an integer",
        cal_quarter INT
            COMMENT "Quarter as an integer",
        cal_month INT
            COMMENT "Month as an integer",
        cal_day INT
            COMMENT "Day of the month as an integer",

        day_of_year INT
            COMMENT "Day of the year as an integer",
        day_of_week INT
            COMMENT "ISO day of the week as an integer - 1 = Monday, 7 = Sunday",
        weeknum INT
            COMMENT "Week number as an integer",
            -- TODO: there's an ISO standard for how to calculate this... reference this here: https://en.wikipedia.org/wiki/ISO_week_date

        -- strings
        date_ISO STRING
            COMMENT "Date as an ISO 8601 string",
        date_short STRING
            COMMENT "Date as dd-MMM-yyyy",
        date_long STRING
            COMMENT "Date as dd MMMMM yyyy",

        year_half STRING
            COMMENT "Year and Half as yyyy-HH",
        year_quarter STRING
            COMMENT "Year and Quarter as yyyy-QQ",
        year_month STRING
            COMMENT "Year and Month as yyyy-MM",
        year_month_short STRING
            COMMENT "Year and Month as yyyy-MMM",

        half_name STRING
            COMMENT "Half as HH",
        quarter_name STRING
            COMMENT "Quarter as QQ",
        month_short_name STRING
            COMMENT "Month as MMM",
        month_full_name STRING
            COMMENT "Month as MMMM",
        month_padded STRING
            COMMENT "Month as MM with leading 0 if applicable",
        day_short_name STRING
            COMMENT "Day as ddd",
        day_full_name STRING
            COMMENT "Day as dddd",
        day_padded STRING
            COMMENT "Day as dd with leading 0 if applicable",
        day_ordinal STRING
            COMMENT "Day with ordinal letters st, nd, rd, th",

        -- SAP idiosyncratic formats
        sap_year_quarter STRING
            COMMENT "year_quarter value from SAP",
        sap_year_month STRING
            COMMENT "year_month value from SAP",
        sap_year_week STRING
            COMMENT "year_week value from SAP",
        sap_year_day STRING
            COMMENT "year_day value from SAP",

        -- date demarcations
        first_date_of_year DATE
            COMMENT "1 Jan for the year",
        last_date_of_year DATE
            COMMENT "31 Dec for the year",
        first_date_of_half DATE
            COMMENT "1 Jan/Jul for the half",
        last_date_of_half DATE
            COMMENT "30-Jun/31-Dec for the half",
        first_date_of_quarter DATE
            COMMENT "1 Jan/Apr/Jul/Oct for the quarter",
        last_date_of_quarter DATE
            COMMENT "31-Mar/30-Jun/30-Sep/31-Dec for the quarter",
        first_date_of_month DATE
            COMMENT "1st of the month",
        last_date_of_month DATE
            COMMENT "Last day of the month",

        -- CONSIDER: can also do "first_date_of_previous_year" and "first_date_of_next_year" etc
        -- and optionally hide this using Views if consuming application doesn't need it

        first_weekday_of_year DATE
            COMMENT "First Monday of the year",
        last_weekday_of_year DATE
            COMMENT "Final Friday of the year",
        first_weekday_of_half DATE
            COMMENT "First Monday of the half",
        last_weekday_of_half DATE
            COMMENT "Final Friday of the half",
        first_weekday_of_quarter DATE
            COMMENT "First Monday of the quarter",
        last_weekday_of_quarter DATE
            COMMENT "Final Friday of the quarter",
        first_weekday_of_month DATE
            COMMENT "First Monday of the month",
        last_weekday_of_month DATE
            COMMENT "Final Friday of the month",

        -- booleans = great for quick filtering in consuming apps
        is_weekday BOOLEAN
            COMMENT "Is Mon-Fri",
        is_weekend BOOLEAN
            COMMENT "Is Sat-Sun",
        is_leap_year BOOLEAN
            COMMENT "Is a leap year",
        is_leap_day BOOLEAN
            COMMENT "Is 29th Feb",
        is_first_day_of_year BOOLEAN
            COMMENT "Is 1st Jan",
        is_first_day_of_half BOOLEAN
            COMMENT "Is 1st Jan/Jul/Apr/Oct",
        is_first_day_of_quarter BOOLEAN
            COMMENT "Is 1st Jan/Apr/Jul/Oct",
        is_first_day_of_month BOOLEAN
            COMMENT "Is 1st of the month",
        is_first_day_of_week BOOLEAN
            COMMENT "Is Monday",
        is_last_day_of_year BOOLEAN
            COMMENT "Is 31st Dec",
        is_last_day_of_half BOOLEAN
            COMMENT "Is 30-Jun/31-Dec",
        is_last_day_of_quarter BOOLEAN
            COMMENT "Is 31-Mar/30-Jun/30-Sep/31-Dec",
        is_last_day_of_month BOOLEAN
            COMMENT "Is Last day of the month",
        is_last_day_of_week BOOLEAN
            COMMENT "Is Sunday",

        is_first_weekday_of_year BOOLEAN
            COMMENT "Is first Monday of the year",
        is_first_weekday_of_half BOOLEAN
            COMMENT "Is first Monday of the half",
        is_first_weekday_of_quarter BOOLEAN
            COMMENT "Is first Monday of the quarter",
        is_first_weekday_of_month BOOLEAN
            COMMENT "Is first Monday of the month",
        is_last_weekday_of_year BOOLEAN
            COMMENT "Is final Friday of the year",
        is_last_weekday_of_half BOOLEAN
            COMMENT "Is final Friday of the half",
        is_last_weekday_of_quarter BOOLEAN
            COMMENT "Is final Friday of the quarter",
        is_last_weekday_of_month BOOLEAN
            COMMENT "Is final Friday of the month",
        is_last_weekday_of_week BOOLEAN
            COMMENT "Is Friday",

        -- metadata
        __etl_keys_fprint BIGINT,
        __etl_row_fprint BIGINT,
        __etl_effective_from TIMESTAMP,
        __etl_effective_to TIMESTAMP,
        __etl_is_active BOOLEAN,
        __etl_is_deleted BOOLEAN

    )
    CLUSTER BY
        AUTO;
''')