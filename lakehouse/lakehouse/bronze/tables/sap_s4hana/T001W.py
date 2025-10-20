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

table_name = 't001w'

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
        WERKS string,
        NAME1 string,
        BWKEY string,
        KUNNR string,
        LIFNR string,
        FABKL string,
        NAME2 string,
        STRAS string,
        PFACH string,
        PSTLZ string,
        ORT01 string,
        EKORG string,
        VKORG string,
        CHAZV string,
        KKOWK string,
        KORDB string,
        BEDPL string,
        LAND1 string,
        REGIO string,
        COUNC string,
        CITYC string,
        ADRNR string,
        IWERK string,
        TXJCD string,
        VTWEG string,
        SPART string,
        SPRAS string,
        WKSOP string,
        AWSLS string,
        CHAZV_OLD string,
        VLFKZ string,
        BZIRK string,
        ZONE1 string,
        TAXIW string,
        BZQHL string,
        LET01 decimal(38,18),
        LET02 decimal(38,18),
        LET03 decimal(38,18),
        TXNAM_MA1 string,
        TXNAM_MA2 string,
        TXNAM_MA3 string,
        BETOL string,
        J_1BBRANCH string,
        VTBFI string,
        FPRFW string,
        ACHVM string,
        DVSART string,
        NODETYPE string,
        NSCHEMA string,
        PKOSA string,
        MISCH string,
        MGVUPD string,
        VSTEL string,
        MGVLAUPD string,
        MGVLAREVAL string,
        SOURCING string,
        FSH_MG_ARUN_REQ string,
        FSH_SEAIM string,
        FSH_BOM_MAINTENANCE string,
        FSH_GROUP_PR string,
        ARUN_FIX_BATCH string,
        OILIVAL string,
        OIHVTYPE string,
        OIHCREDIPI string,
        STORETYPE string,
        DEP_STORE string,
        NO_DEFAULT_BATCH_MANAGEMENT string,

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
