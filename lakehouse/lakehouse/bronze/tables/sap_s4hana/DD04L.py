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

table_name = 'dd04l'

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
        ROLLNAME string,
        AS4LOCAL string,
        AS4VERS string,
        DOMNAME string,
        ROUTPUTLEN string,
        MEMORYID string,
        LOGFLAG string,
        HEADLEN string,
        SCRLEN1 string,
        SCRLEN2 string,
        SCRLEN3 string,
        ACTFLAG string,
        APPLCLASS string,
        AUTHCLASS string,
        AS4USER string,
        AS4DATE string,
        AS4TIME string,
        DTELMASTER string,
        RESERVEDTE string,
        DTELGLOBAL string,
        SHLPNAME string,
        SHLPFIELD string,
        DEFFDNAME string,
        DATATYPE string,
        LENG string,
        DECIMALS string,
        OUTPUTLEN string,
        LOWERCASE string,
        SIGNFLAG string,
        CONVEXIT string,
        VALEXI string,
        ENTITYTAB string,
        REFKIND string,
        REFTYPE string,
        PROXYTYPE string,
        LTRFLDDIS string,
        BIDICTRLC string,
        NOHISTORY string,
        ABAP_LANGUAGE_VERSION string,

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
