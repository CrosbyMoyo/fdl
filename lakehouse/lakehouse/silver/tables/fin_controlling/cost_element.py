# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.silver_catalog}.fin_controlling.cost_element (
        -- key columns
        cost_element STRING 
            COMMENT "Cost Element",
        chart_of_accounts STRING
            COMMENT "Chart of Accounts",

        -- payload columns
        cost_element_description_long STRING
            COMMENT "Long Description for the Cost Element",
        primary_cost STRING
            COMMENT "Primary Cost",
        primary_cost_description STRING
            COMMENT "Primary Cost Description",
        total_opex STRING
            COMMENT "Total Opex",
        total_opex_description STRING
            COMMENT "Total Opex Description",

        -- metadata columns
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
        COMMENT 'Cost Center Master Record'
        CLUSTER BY 
        AUTO;

''')