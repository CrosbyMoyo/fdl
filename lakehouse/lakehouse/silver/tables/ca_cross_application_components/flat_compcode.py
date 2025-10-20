# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.silver_catalog}.ca_cross_application_components.flat_compcode (
        company_code STRING
            COMMENT 'The company code for the company that this record is for',
        display_name STRING
            COMMENT 'The name for the company',
        reporting_entity STRING
            COMMENT 'The reporting entity for the company',
        geo_region STRING
            COMMENT 'The region for the company',
        vivo_group STRING
            COMMENT 'The group for the company',
        region_alt_1 STRING
            COMMENT 'The region for the company (alternative 1)',
        entity_grouping_level_top STRING
            COMMENT 'The top level hierarchy for the company (grouping level top)',
        entity_grouping_level_0 STRING
            COMMENT 'The hierarchy level 0 for the company (grouping level 0)',
        entity_grouping_level_1 STRING
            COMMENT 'The hierarchy level 1 for the company (grouping level 1)',
        entity_grouping_level_2_geographical STRING
            COMMENT 'The hierarchy level 2 for the company (grouping level 2 - geographical)',
        entity_grouping_level_3_vp_reporting STRING
            COMMENT 'The hierarchy level 3 for the company (grouping level 3 - vp reporting)',
        currency STRING
            COMMENT 'The currency that is used for the company',
        planning_company_code STRING
            COMMENT 'The company code for the company that is used for planning',
        central_credit_country_grouping STRING
            COMMENT 'The credit country grouping for the company',
        reporting_entity_ri STRING
            COMMENT 'The reporting entity for the company (RI)',
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