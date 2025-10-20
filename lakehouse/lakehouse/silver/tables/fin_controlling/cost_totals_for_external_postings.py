# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.silver_catalog}.fin_controlling.cost_totals_for_external_postings (

        --keys
        client STRING
            COMMENT 'client code'
        ,ledger STRING
            COMMENT 'Budget/Planning Ledger'
        ,object_number STRING
            COMMENT 'Object Number'
        ,fiscal_year INT 
            COMMENT 'fiscal year'
        ,value_type STRING 
            COMMENT 'value type'
        ,version_type STRING
            COMMENT 'version type'
        ,cost_element STRING
            COMMENT 'cost element'
        ,co_subkey STRING
            COMMENT 'co subkey'
        ,business_transaction STRING
            COMMENT 'business transaction'
        ,trading_partner_no STRING
            COMMENT 'trading partner number'
        ,trading_partba STRING
            COMMENT 'trading partner business area'
        ,drcr_indicator STRING
            COMMENT 'debit/credit indicator'
        ,transaction_currency STRING 
            COMMENT 'transaction currency'
        ,period_block INT
            COMMENT 'period block'
        ,company_code STRING 
            COMMENT 'company code'
        ,debit_type STRING
            COMMENT 'debit type'
        ,unit_of_measure STRING
            COMMENT 'unit of measure'

        -- payload
        
        ,value_in_obj_crcy1 DECIMAL(18,2) 
            COMMENT 'total plan value 1'
        ,value_in_obj_crcy2 DECIMAL(18,2) 
            COMMENT 'total plan value 2'
        ,value_in_obj_crcy3 DECIMAL(18,2) 
            COMMENT 'total plan value 3'
        ,value_in_obj_crcy4 DECIMAL(18,2) 
            COMMENT 'total plan value 4'
        ,value_in_obj_crcy5 DECIMAL(18,2) 
            COMMENT 'total plan value 5'
        ,value_in_obj_crcy6 DECIMAL(18,2) 
            COMMENT 'total plan value 6'
        ,value_in_obj_crcy7 DECIMAL(18,2) 
            COMMENT 'total plan value 7'
        ,value_in_obj_crcy8 DECIMAL(18,2) 
            COMMENT 'total plan value 8'
        ,value_in_obj_crcy9 DECIMAL(18,2) 
            COMMENT 'total plan value 9'
        ,value_in_obj_crcy10 DECIMAL(18,2) 
            COMMENT 'total plan value 10'
        ,value_in_obj_crcy11 DECIMAL(18,2)
            COMMENT 'total plan value 11'
        ,value_in_obj_crcy12 DECIMAL(18,2)
            COMMENT 'total plan value 12'

        ,valcoarea_crcy1 DECIMAL(18,2) 
            COMMENT 'budget value 1'
        ,valcoarea_crcy2 DECIMAL(18,2) 
            COMMENT 'budget value 2'
        ,valcoarea_crcy3 DECIMAL(18,2) 
            COMMENT 'budget value 3'
        ,valcoarea_crcy4 DECIMAL(18,2) 
            COMMENT 'budget value 4'
        ,valcoarea_crcy5 DECIMAL(18,2) 
            COMMENT 'budget value 5'
        ,valcoarea_crcy6 DECIMAL(18,2)  
            COMMENT 'budget value 6'
        ,valcoarea_crcy7 DECIMAL(18,2) 
            COMMENT 'budget value 7'
        ,valcoarea_crcy8 DECIMAL(18,2)
            COMMENT 'budget value 8' 
        ,valcoarea_crcy9 DECIMAL(18,2) 
            COMMENT 'budget value 9'
        ,valcoarea_crcy10 DECIMAL(18,2) 
            COMMENT 'budget value 10'
        ,valcoarea_crcy11 DECIMAL(18,2) 
            COMMENT 'budget value 11'
        ,valcoarea_crcy12 DECIMAL(18,2)        
            COMMENT 'budget value 12' 

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