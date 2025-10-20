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

table_name = 'cepc'

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
        PRCTR string,
        DATBI string,
        KOKRS string,
        DATAB string,
        ERSDA string,
        USNAM string,
        MERKMAL string,
        ABTEI string,
        VERAK string,
        VERAK_USER string,
        WAERS string,
        NPRCTR string,
        LAND1 string,
        ANRED string,
        NAME1 string,
        NAME2 string,
        NAME3 string,
        NAME4 string,
        ORT01 string,
        ORT02 string,
        STRAS string,
        PFACH string,
        PSTLZ string,
        PSTL2 string,
        SPRAS string,
        TELBX string,
        TELF1 string,
        TELF2 string,
        TELFX string,
        TELTX string,
        TELX1 string,
        DATLT string,
        DRNAM string,
        KHINR string,
        BUKRS string,
        VNAME string,
        RECID string,
        ETYPE string,
        TXJCD string,
        REGIO string,
        KVEWE string,
        KAPPL string,
        KALSM string,
        LOGSYSTEM string,
        LOCK_IND string,
        PCA_TEMPLATE string,
        SEGMENT string,
        EEW_CEPC_PS_DUMMY string,

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
