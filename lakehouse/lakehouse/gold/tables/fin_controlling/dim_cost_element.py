# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.gold_catalog}.fin_controlling.dim_cost_element
    (
        cost_element_skey BIGINT
            CONSTRAINT dim_cost_element_pk PRIMARY KEY
            COMMENT 'Cost Element Skey',
        cost_element STRING
            COMMENT 'Cost Element',
        chart_of_accounts STRING
            COMMENT 'Chart of Accounts',
        cost_element_description_long STRING
            COMMENT 'Cost Element Long Description',
        primary_cost STRING
            COMMENT 'Primary Cost Hierarchy',
        primary_cost_description STRING
            COMMENT 'Primary Cost Description',
        total_opex STRING
            COMMENT 'Total Opex Hierarchy',
        total_opex_description STRING
            COMMENT 'Total Opex Description',
            
            -- Metadata
        __etl_keys_fprint BIGINT
            COMMENT "xxhash64 of the Business Keys that this record is made up of",
        __etl_row_fprint BIGINT
            COMMENT "the xxhash64 of all the columns that make up the row payload",
        __etl_effective_from DATE
            COMMENT "Date that row is effective from",
        __etl_effective_to DATE
            COMMENT "Date that row is effective to, or NULL for active record",
        __etl_is_active BOOLEAN
            COMMENT "flag indicating the active record. Note: there should only be 1 _is_active for any _etl_keys_fprint",
        __etl_is_deleted BOOLEAN
            COMMENT "showing if the record has been deleted from the source system"
    )
    CLUSTER BY 
        AUTO;
''')