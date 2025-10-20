# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.silver_catalog}.fin_finance.capex_ceilings (

        -- keys
        ceiling_id INT
            COMMENT 'primary key'      
        ,company_code STRING
            COMMENT 'company code'
            
        -- payload
        ,budget_year INT
            COMMENT 'year of the budget'
        ,country_code STRING
            COMMENT 'country code' 
        ,line_of_business_name STRING
            COMMENT 'name of line of business'
        ,currency_key STRING
            COMMENT 'currency code'
        ,lob1_Ceiling STRING
            COMMENT 'ceiling for line of business'
        ,subtype_name STRING
            COMMENT 'name of sub type'
        ,ceiling DECIMAL(36,12)
            COMMENT 'ceiling for line of business'
        
        -- metadata
        ,__etl_keys_fprint BIGINT
            COMMENT 'xxhash64 of the Business Keys that this record is made up of (or, where the table is the result of joins, it is the fields that make the record unique)'
        ,__etl_row_fprint BIGINT
            COMMENT ' the xxhash64 of all the columns that make up the row payload (i.e. all the non-key, and non-metadata columns). Note: all columns must be NOT NULL for the hash to calculate properly.'
        ,__etl_effective_from DATE
            COMMENT 'date (as DATE datatype) that row is effective from. For an updated record this is the previous _effective_to date + 1 day.'
        ,__etl_effective_to DATE
            COMMENT 'date (as DATE datatype) that row is effective to, or NULL for active record'
        ,__etl_is_active BOOLEAN
            COMMENT 'boolean flag indicating the active record. Note: there should only be 1 _is_active for any _etl_keys_fprint'
        ,__etl_is_deleted BOOLEAN
            COMMENT 'boolean showing if the record has been deleted from the source system'
)
CLUSTER BY AUTO;
''')