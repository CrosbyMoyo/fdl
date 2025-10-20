# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.gold_catalog}.fin_controlling.dim_cost_center
    (
        cost_center_skey BIGINT
            CONSTRAINT dim_cost_center_pk PRIMARY KEY
            COMMENT 'Cost Center Skey',
        controlling_area STRING
            COMMENT 'Controlling Area',
        cost_center STRING
            COMMENT 'Cost Center',
        valid_to DATE
            COMMENT 'Valid To',
        company_code STRING
            COMMENT 'Company Code',
        budget_holder STRING
            COMMENT 'Budget Holder',
        cost_center_description STRING
            COMMENT 'Cost Center Description',
        valid_from DATE
            COMMENT 'Valid From',
        cost_center_category STRING
            COMMENT 'Cost Center Category',
        cost_center_category_description STRING
            COMMENT 'Cost Center Category Description',
        profit_center STRING
            COMMENT 'Profit Center',
        department STRING
            COMMENT 'Department',
            
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