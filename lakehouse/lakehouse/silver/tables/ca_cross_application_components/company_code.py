# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.silver_catalog}.ca_cross_application_components.company_code (
        company_code STRING
            COMMENT 'The company code that the company uses', 
        company_name STRING
            COMMENT 'The company name that the company uses',
        country_key STRING
            COMMENT 'The country that the company is in',
        city STRING
            COMMENT 'The city that the company is in',
        currency_key STRING
            COMMENT 'The currency that the country uses',
        preferred_language STRING
            COMMENT 'The language that the country prefers to use',
        chart_of_accounts STRING
            COMMENT 'The chart of accounts that the company uses',
        credit_control_area STRING
            COMMENT 'The credit control area that the company is in',
        display_name STRING
            COMMENT 'Often company code name in capitals but could have a different description. Is maintained by Flat File load.',
        reporting_entity STRING
            COMMENT 'Entity for statutary reporting. Attribute assigned by flat-file load',
        geo_region STRING
            COMMENT 'Geographical region. Attribute assigned by flat-file load.',
        vivo_group STRING
            COMMENT 'Grouping of company code for statutory and MI reporting. For example, "Vivo", "SVL", JV".',
        entity_grouping_level_top STRING
            COMMENT 'Entity Hierarchy - top level. Attribute assigned by flat-file load',        
        entity_grouping_level_0 STRING
            COMMENT 'Entity Hierarchy - level 0. Attribute assigned by flat-file load',
        entity_grouping_level_1 STRING
            COMMENT 'Entity Hierarchy - level 1. Attribute assigned by flat-file load',
        entity_grouping_level_2_geographical STRING
            COMMENT 'Entity Hierarchy - level 2. Attribute assigned by flat-file load',
        entity_grouping_level_3_vp_reporting STRING
            COMMENT 'Entity Hierarchy - level 3. Attribute assigned by flat-file load',
        operating_unit STRING
            COMMENT 'Referred to as "OU". The value is derived from the company code region and reporting entity attributes. CASE WHEN Region in ("East","South","West","Maghreb & Indian Ocean") THEN LEFT("Reporting Entity",50) ELSE "Non-Operating" END',
        region_alternative_2 STRING
            COMMENT 'CASE WHEN Region = "East & South" OR Region = "West" or Region = "Maghreb & Indian Ocean" or Region = "East" or Region = "South" THEN Region ELSE "Non-Operating" END',
        planning_company_code STRING
            COMMENT 'attribute assigned by flat-file load',
        central_credit_country_grouping  STRING
            COMMENT 'attribute assigned by flat-file load',
        reporting_entity_ri STRING
            COMMENT 'attribute assigned by flat-file load',
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