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

table_name = 'dd03l'

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
        TABNAME string,
        FIELDNAME string,
        AS4LOCAL string,
        AS4VERS string,
        `POSITION` string,
        KEYFLAG string,
        MANDATORY string,
        ROLLNAME string,
        CHECKTABLE string,
        ADMINFIELD string,
        INTTYPE string,
        INTLEN string,
        REFTABLE string,
        PRECFIELD string,
        REFFIELD string,
        CONROUT string,
        NOTNULL string,
        DATATYPE string,
        LENG string,
        DECIMALS string,
        DOMNAME string,
        SHLPORIGIN string,
        TABLETYPE string,
        DEPTH string,
        COMPTYPE string,
        REFTYPE string,
        LANGUFLAG string,
        DBPOSITION string,
        `ANONYMOUS` string,
        OUTPUTSTYLE string,
        SRS_ID int,

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
