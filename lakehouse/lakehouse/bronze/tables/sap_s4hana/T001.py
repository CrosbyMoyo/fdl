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

table_name = 't001'

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
        BUKRS string,
        BUTXT string,
        ORT01 string,
        LAND1 string,
        WAERS string,
        SPRAS string,
        KTOPL string,
        WAABW string,
        PERIV string,
        KOKFI string,
        RCOMP string,
        ADRNR string,
        STCEG string,
        FIKRS string,
        XFMCO string,
        XFMCB string,
        XFMCA string,
        TXJCD string,
        FMHRDATE string,
        XTEMPLT string,
        TRANSIT_PLANT string,
        BUVAR string,
        FDBUK string,
        XFDIS string,
        XVALV string,
        XSKFN string,
        KKBER string,
        XMWSN string,
        MREGL string,
        XGSBE string,
        XGJRV string,
        XKDFT string,
        XPROD string,
        XEINK string,
        XJVAA string,
        XVVWA string,
        XSLTA string,
        XFDMM string,
        XFDSD string,
        XEXTB string,
        EBUKR string,
        KTOP2 string,
        UMKRS string,
        BUKRS_GLOB string,
        FSTVA string,
        OPVAR string,
        XCOVR string,
        TXKRS string,
        WFVAR string,
        XBBBF string,
        XBBBE string,
        XBBBA string,
        XBBKO string,
        XSTDT string,
        MWSKV string,
        MWSKA string,
        IMPDA string,
        XNEGP string,
        XKKBI string,
        WT_NEWWT string,
        PP_PDATE string,
        INFMT string,
        FSTVARE string,
        KOPIM string,
        DKWEG string,
        OFFSACCT string,
        BAPOVAR string,
        XCOS string,
        XCESSION string,
        XSPLT string,
        SURCCM string,
        DTPROV string,
        DTAMTC string,
        DTTAXC string,
        DTTDSP string,
        DTAXR string,
        XVATDATE string,
        PST_PER_VAR string,
        XBBSC string,
        F_OBSOLETE string,
        FM_DERIVE_ACC string,

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
