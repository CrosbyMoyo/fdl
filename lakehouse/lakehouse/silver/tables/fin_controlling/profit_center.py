# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.silver_catalog}.fin_controlling.profit_center (
        profit_center STRING 
            COMMENT 'Unique identifier for the profit center',
        profit_center_description STRING 
            COMMENT 'Detailed description of the profit center',
        controlling_area STRING 
            COMMENT 'Defines the controlling area grouping profit centers',
        valid_from DATE
            COMMENT 'SAP entry of the record',
        valid_to DATE
            COMMENT 'SAP exit of the record',
        segment STRING 
            COMMENT 'Business segment associated with the profit center',
        segment_description STRING 
            COMMENT 'Description of the business segment',
        line_of_business STRING 
            COMMENT 'Primary line of business linked to the profit center',
        line_of_business_description STRING 
            COMMENT 'Detailed description of the line of business',
        line_of_business_1 STRING 
            COMMENT 'Secondary line of business for further classification',
        line_of_business_1_description STRING 
            COMMENT 'Description of the secondary line of business',
        volume_flag_ind BOOLEAN 
            COMMENT 'Indicator flag for volume-related business classification',
        sales_organization STRING 
            COMMENT 'Sales organization responsible for profit center',
        sales_organization_description STRING 
            COMMENT 'Description of the sales organization',
        distribution_channel STRING 
            COMMENT 'Channel used for product distribution',
        distribution_channel_description STRING 
            COMMENT 'Detailed description of the distribution channel',
        division STRING 
            COMMENT 'Organizational division within the company',
        division_description STRING 
            COMMENT 'Detailed description of the division',
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