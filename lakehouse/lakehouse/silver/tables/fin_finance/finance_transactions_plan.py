# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.silver_catalog}.fin_finance.finance_transactions_plan (
        -- Keys 
        plan_version STRING COMMENT 'The version of the plan',
        date_key DATE COMMENT 'Transaction plan date as the last day of the month',
        gl_account STRING COMMENT 'GL Account',
        vcode STRING COMMENT 'V Code',
        company_code STRING COMMENT 'Company Code',
        customer STRING COMMENT 'Customer',
        profit_center STRING COMMENT 'Profit Center',
        material STRING COMMENT 'Material',
        line_of_business STRING COMMENT 'The line of business code',
        cost_center STRING COMMENT 'Cost Center',
        
        -- Payload 
        currency_key STRING COMMENT 'Currency key of the company',
        controlling_area STRING COMMENT 'The controlling area',
        collection_specialist STRING COMMENT 'Collection Specialist',
        collection_segment STRING COMMENT 'Collection Segment',
        vcode_amount_local DOUBLE COMMENT 'Amount Local',
        quantity DOUBLE COMMENT 'Quantity',
        amount_local_currency DOUBLE COMMENT 'Amount Local Currency',
        volume_kg DOUBLE COMMENT 'Volume KGs',
        volume_litres_l20 DOUBLE COMMENT 'Volume Litres',
        volume_issued_litres_l20 DOUBLE COMMENT 'Volume Issued Litres',
        volume_flag_ind BOOLEAN COMMENT 'Volume Flag Indicator',
        
        -- Metadata
        __etl_keys_fprint BIGINT
            COMMENT 'xxhash64 of the Business Keys that this record is made up of (or, where the table is the result of joins, it is the fields that make the record unique)',
        __etl_row_fprint BIGINT
            COMMENT ' the xxhash64 of all the columns that make up the row payload (i.e. all the non-key, and non-metadata columns). Note: all columns must be NOT NULL for the hash to calculate properly.',
        __etl_effective_from DATE
            COMMENT 'DATE (as DATE datatype) that row is effective from. For an updated record this is the previous _effective_to DATE + 1 day.',
        __etl_effective_to DATE
            COMMENT 'DATE (as DATE datatype) that row is effective to, or NULL for active record',
        __etl_is_active BOOLEAN
            COMMENT 'boolean flag indicating the active record. Note: there should only be 1 _is_active for any _etl_keys_fprint',
        __etl_is_deleted BOOLEAN
            COMMENT 'boolean showing if the record has been deleted from the source system'
    ) 
    CLUSTER BY AUTO;
''')