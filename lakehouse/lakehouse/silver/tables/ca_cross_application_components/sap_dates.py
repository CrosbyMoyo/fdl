# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.silver_catalog}.ca_cross_application_components.sap_dates (

        -- keys
        calendar_date DATE
            COMMENT "Date as date",

        -- payload
        calendar_year INT
            COMMENT "Calendar year",
        calendar_month INT
            COMMENT "Calendar month",
        calendar_quarter INT
            COMMENT "Calendar quarter",
        calendar_week INT
            COMMENT "Calendar week",
        calendar_day INT
            COMMENT "Calendar day",
        week_day INT
            COMMENT "Weekday",
        year_week INT
            COMMENT "Year and week",
        year_quarter INT
            COMMENT "Year and quarter",
        year_month INT
            COMMENT "Year and month",
        first_day_of_month DATE
            COMMENT "First day of month",
        last_day_of_month DATE
            COMMENT "Last day of month",
        half_year INT
            COMMENT "Calendar half",
        first_day_of_week_date DATE
            COMMENT "First day of week date",
        calendar_day_of_year INT
            COMMENT "Day number in year",
        year_day INT
            COMMENT "Year and day",

        -- metadata
        __etl_keys_fprint BIGINT
            COMMENT 'xxhash64 of the Business Keys that this record is made up of (or, where the table is the result of joins, it is the fields that make the record unique)',
        __etl_row_fprint BIGINT
            COMMENT ' the xxhash64 of all the columns that make up the row payload (i.e. all the non-key, and non-metadata columns). Note: all columns must be NOT NULL for the hash to calculate properly.',
        __etl_effective_from DATE
            COMMENT 'date (as DATE datatype) that row is effective from. For an updated record this is the previous _effective_to date + 1 day.',
        __etl_effective_to DATE
            COMMENT 'date (as DATE datatype) that row is effective to, or NULL for active record',
        __etl_is_active BOOLEAN
            COMMENT 'boolean flag indicating the active record. Note: there should only be 1 _is_active for any _etl_keys_fprint',
        __etl_is_deleted BOOLEAN
            COMMENT 'boolean showing if the record has been deleted from the source system'
    )
    COMMENT "Date table containing dates between 2010 and 2035"
    CLUSTER BY
        AUTO;
''')