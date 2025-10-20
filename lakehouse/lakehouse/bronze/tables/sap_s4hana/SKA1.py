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

table_name = 'ska1'

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
        KTOPL string,
        SAKNR string,
        XBILK string,
        SAKAN string,
        BILKT string,
        ERDAT string,
        ERNAM string,
        GVTYP string,
        KTOKS string,
        MUSTR string,
        VBUND string,
        XLOEV string,
        XSPEA string,
        XSPEB string,
        XSPEP string,
        MCOD1 string,
        FUNC_AREA string,
        GLACCOUNT_TYPE string,
        LAST_CHANGED_TS decimal(38,18),
        GLACCOUNT_SUBTYPE string,
        MAIN_SAKNR string,

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
