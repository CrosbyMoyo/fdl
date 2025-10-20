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

table_name = 'csks'

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
        KOKRS string,
        KOSTL string,
        DATBI string,
        DATAB string,
        BKZKP string,
        PKZKP string,
        BUKRS string,
        GSBER string,
        KOSAR string,
        VERAK string,
        VERAK_USER string,
        WAERS string,
        KALSM string,
        TXJCD string,
        PRCTR string,
        WERKS string,
        LOGSYSTEM string,
        ERSDA string,
        USNAM string,
        BKZKS string,
        BKZER string,
        BKZOB string,
        PKZKS string,
        PKZER string,
        VMETH string,
        MGEFL string,
        ABTEI string,
        NKOST string,
        KVEWE string,
        KAPPL string,
        KOSZSCHL string,
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
        REGIO string,
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
        CCKEY string,
        KOMPL string,
        STAKZ string,
        OBJNR string,
        FUNKT string,
        AFUNK string,
        CPI_TEMPL string,
        CPD_TEMPL string,
        FUNC_AREA string,
        SCI_TEMPL string,
        SCD_TEMPL string,
        SKI_TEMPL string,
        SKD_TEMPL string,
        EEW_CSKS_PS_DUMMY string,
        VNAME string,
        RECID string,
        ETYPE string,
        JV_OTYPE string,
        JV_JIBCL string,
        JV_JIBSA string,
        FERC_IND string,
        BUDGET_CARRYING_COST_CTR string,
        AVC_PROFILE string,
        AVC_ACTIVE string,
        FUND string,
        GRANT_ID string,
        FUND_FIX_ASSIGNED string,
        GRANT_FIX_ASSIGNED string,
        FUNC_AREA_FIX_ASSIGNED string,

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
