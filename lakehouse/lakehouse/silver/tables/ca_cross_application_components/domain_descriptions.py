# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.silver_catalog}.ca_cross_application_components.domain_descriptions (
        -- Keys
        domain_name STRING
            COMMENT 'The company code that the company uses', 
        language_key STRING
            COMMENT 'The company name that the company uses',
        activation_state STRING
            COMMENT 'The country that the company is in',
        value_key STRING
            COMMENT 'The city that the company is in',
        domain_version STRING
            COMMENT 'The currency that the country uses',

        -- Payload
        short_description STRING
            COMMENT 'The language that the country prefers to use',
        lower_limit STRING
            COMMENT 'The chart of accounts that the company uses',
        upper_limit STRING
            COMMENT 'The credit control area that the company is in',
        lower_value STRING
            COMMENT 'Often company code name in capitals but could have a different description. Is maintained by Flat File load.',

        -- Metadata
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