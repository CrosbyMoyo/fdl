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

table_name = 'marm'

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
        MATNR string,
        MEINH string,
        UMREZ decimal(38,18),
        UMREN decimal(38,18),
        EANNR string,
        EAN11 string,
        NUMTP string,
        LAENG decimal(38,18),
        BREIT decimal(38,18),
        HOEHE decimal(38,18),
        MEABM string,
        VOLUM decimal(38,18),
        VOLEH string,
        BRGEW decimal(38,18),
        GEWEI string,
        MESUB string,
        ATINN string,
        MESRT string,
        XFHDW string,
        XBEWW string,
        KZWSO string,
        MSEHI string,
        BFLME_MARM string,
        GTIN_VARIANT string,
        NEST_FTR decimal(38,18),
        MAX_STACK int,
        CAPAUSE decimal(38,18),
        TY2TQ string,
        DUMMY_UOM_INCL_EEW_PS string,
        `/CWM/TY2TQ` string,
        PCBUT string,
        TOP_LOAD_FULL decimal(38,18),
        TOP_LOAD_FULL_UOM string,
        `/STTPEC/NCODE` string,
        `/STTPEC/NCODE_TY` string,
        `/STTPEC/RCODE` string,
        `/STTPEC/SERUSE` string,
        `/STTPEC/SYNCCHG` string,
        `/STTPEC/SERNO_MANAGED` string,
        `/STTPEC/SERNO_PROV_BUP` string,
        `/STTPEC/UOM_SYNC` string,
        `/STTPEC/SER_GTIN` string,

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
