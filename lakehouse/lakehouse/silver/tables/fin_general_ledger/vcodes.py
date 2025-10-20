# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.silver_catalog}.fin_general_ledger.vcodes (
        -- key
        vcode STRING 
            COMMENT 'GL Account identifier',

        -- payloads    
        description STRING
            COMMENT 'Description of the GL Account',
        c1_relevant_flag BOOLEAN
            COMMENT 'Flag indicating if Vcode is relevant for C1',
        c2_relevant_flag BOOLEAN
            COMMENT 'Flag indicating if Vcode is relevant for C2',
        c3_relevant_flag BOOLEAN
            COMMENT 'Flag indicating if Vcode is relevant for C3',
        c4_relevant_flag BOOLEAN
            COMMENT 'Flag indicating if Vcode is relevant for C4',
        net_income_relevant_flag BOOLEAN
            COMMENT 'Flag indicating if Vcode is relevant for Net Income',
        local_ebitda_relevant_flag BOOLEAN
            COMMENT 'Flag indicating if Vcode is relevant for Local EBITDA',
        local_opex STRING
            COMMENT 'Local Opex',
        opex_type STRING
            COMMENT 'Type of Opex',
        direct_contribution_relevant_flag BOOLEAN
            COMMENT 'Flag indicating if Vcode is relevant for Direct Contribution',
        indirect_contribution_relevant_flag BOOLEAN
            COMMENT 'Flag indicating if Vcode is relevant for Indirect Contribution',
        vcode_sort_order INT
            COMMENT 'Sort order for Vcode',
        
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
    CLUSTER BY
        AUTO;
''')