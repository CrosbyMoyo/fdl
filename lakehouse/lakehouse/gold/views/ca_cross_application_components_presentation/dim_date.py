# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.ca_cross_application_components_presentation.dim_date
    AS
    SELECT
        dd.date_key
        ,dd.date_number
        ,dd.cal_year
        ,dd.cal_half
        ,dd.cal_quarter
        ,dd.cal_month
        ,dd.cal_day
        ,dd.day_of_year
        ,dd.day_of_week
        ,dd.weeknum
        ,dd.date_ISO
        ,dd.date_short
        ,dd.date_long
        ,dd.year_half
        ,dd.year_quarter
        ,dd.year_month
        ,dd.year_month_short
        ,dd.half_name
        ,dd.quarter_name
        ,dd.month_short_name
        ,dd.month_full_name
        ,dd.month_padded
        ,dd.day_short_name
        ,dd.day_full_name
        ,dd.day_padded
        ,dd.day_ordinal
        ,dd.sap_year_quarter
        ,dd.sap_year_month
        ,dd.sap_year_week
        ,dd.sap_year_day
        ,dd.first_date_of_year
        ,dd.last_date_of_year
        ,dd.first_date_of_half
        ,dd.last_date_of_half
        ,dd.first_date_of_quarter
        ,dd.last_date_of_quarter
        ,dd.first_date_of_month
        ,dd.last_date_of_month
        ,dd.first_weekday_of_year
        ,dd.last_weekday_of_year
        ,dd.first_weekday_of_half
        ,dd.last_weekday_of_half
        ,dd.first_weekday_of_quarter
        ,dd.last_weekday_of_quarter
        ,dd.first_weekday_of_month
        ,dd.last_weekday_of_month
        ,dd.is_weekday
        ,dd.is_weekend
        ,dd.is_leap_year
        ,dd.is_leap_day
        ,dd.is_first_day_of_year
        ,dd.is_first_day_of_half
        ,dd.is_first_day_of_quarter
        ,dd.is_first_day_of_month
        ,dd.is_first_day_of_week
        ,dd.is_last_day_of_year
        ,dd.is_last_day_of_half
        ,dd.is_last_day_of_quarter
        ,dd.is_last_day_of_month
        ,dd.is_last_day_of_week
        ,dd.is_first_weekday_of_year
        ,dd.is_first_weekday_of_half
        ,dd.is_first_weekday_of_quarter
        ,dd.is_first_weekday_of_month
        ,dd.is_last_weekday_of_year
        ,dd.is_last_weekday_of_half
        ,dd.is_last_weekday_of_quarter
        ,dd.is_last_weekday_of_month
        ,dd.is_last_weekday_of_week
    FROM
        {env_vars.gold_catalog}.ca_cross_application_components.dim_date AS dd;

''')

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'''
        GRANT ALL PRIVILEGES
        ON VIEW {env_vars.gold_catalog}.ca_cross_application_components_presentation.dim_date
        TO `data-engineers`;
    ''')