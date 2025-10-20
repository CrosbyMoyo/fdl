# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

dbutils.widgets.dropdown(
    name = 'table_operation',
    defaultValue = 'CREATE TABLE IF NOT EXISTS', 
    choices = [
        'CREATE TABLE IF NOT EXISTS',
        'CREATE OR REPLACE'
    ],
    label = '1- Table Operation'
)

table_operation = dbutils.widgets.get('table_operation')

# COMMAND ----------

table_name = 'text_rawexpos'

# COMMAND ----------

spark,sql(f'''
    ALTER TABLE {env_vars.bronze_catalog}.sap_s4hana.{table_name}
    SET TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled');
''')

# COMMAND ----------

spark.sql(f'''
    -- CREATE TABLE IF NOT EXISTS
    {table_operation} {env_vars.bronze_catalog}.sap_s4hana.{table_name}
    (
        MANDT string,
        OS_GUID binary,
        EXPOSURE_ID string,
        `VERSION` string,
        LOG_SYSTEM string,
        EXPOSURE_ORIGIN string,
        EXT_DOC_NO string,
        ORIGIN_TSTMP decimal(38,18),
        CREATION_IND string,
        REFERENCE_ID string,
        ROOT_DOCUMENT_ID string,
        INVOICING_STATUS string,
        DELIVERY_STATUS string,
        EXP_FLOW_TYPE string,
        TRANSACTION_CAT string,
        COUNTRY string,
        COMPANY_CODE string,
        ATTRIBUTE_SH01 string,
        ATTRIBUTE_SH02 string,
        ATTRIBUTE_LH01 string,
        ATTRIBUTE_DH01 string,
        BAL_INDICATOR string,
        RELEASE_STATE string,
        CREATION_DATE string,
        CREATION_TIME string,
        CREATION_USER string,
        CREATION_TCODE string,
        LASTCHANGE_DATE string,
        LASTCHANGE_TIME string,
        LASTCHANGE_USER string,
        LASTCHANGE_TCODE string,
        RELEVANCE_DATE string,
        LAST_REL_VERSION string,

        -- metadata columns
        __etl_id BIGINT
            GENERATED ALWAYS AS IDENTITY,
        __etl_bronze_timestamp TIMESTAMP
            DEFAULT current_timestamp(),
        __etl_silver_timestamp TIMESTAMP,
        __etl_source_operation STRING
    )
    CLUSTER BY
        AUTO;
''')

# COMMAND ----------

# add the metadata columns back in
# spark.sql(f'''
#     ALTER TABLE {env_vars.bronze_catalog}.sap_s4hana.{table_name}
#     ADD COLUMNS (
#         __etl_id BIGINT,
#         __etl_bronze_timestamp TIMESTAMP,
#         __etl_silver_timestamp TIMESTAMP,
#         __etl_source_operation STRING
#     );
# ''')
