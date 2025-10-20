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

table_name = 'acdocu'

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
        RCLNT string,
        RLDNR string,
        RDIMEN string,
        RYEAR string,
        DOCNR string,
        DOCLN string,
        RRCTY string,
        RVERS string,
        RTCUR string,
        RHCUR string,
        RKCUR string,
        RUNIT string,
        POPER string,
        FISCYEARPER string,
        DOCCT string,
        RCOMP string,
        RBUNIT string,
        RITCLG string,
        RITEM string,
        RBUPTR string,
        RCONGR string,
        ROBUKRS string,
        SITYP string,
        SUBIT string,
        PLEVL string,
        RPFLG string,
        RTFLG string,
        DOCTY string,
        YRACQ string,
        PRACQ string,
        COICU string,
        UPPCU string,
        TSL decimal(38,18),
        HSL decimal(38,18),
        KSL decimal(38,18),
        MSL string,
        SGTXT string,
        AUTOM string,
        ACTIV string,
        BVORG string,
        BUDAT string,
        WSDAT string,
        REFDOCNR string,
        REFRYEAR string,
        REFDOCLN string,
        REFDOCCT string,
        REFACTIV string,
        TIMESTAMP decimal(38,18),
        CPUDT string,
        CPUTM string,
        USNAM string,
        RVSDOCNR string,
        ORNDOCNR string,
        COIAC string,
        COINR string,
        REVYEAR string,
        AWTYP string,
        AWORG string,
        LOGSYS string,
        KTOPL string,
        RACCT string,
        XBLNR string,
        ZUONR string,
        RCNTR string,
        PRCTR string,
        RFAREA string,
        RBUSA string,
        KOKRS string,
        SEGMENT string,
        SCNTR string,
        PPRCTR string,
        SFAREA string,
        SBUSA string,
        RASSC string,
        PSEGMENT string,
        AUFNR string,
        KUNNR string,
        LIFNR string,
        MATNR string,
        MATKL_MM string,
        WERKS string,
        RMVCT string,
        PS_PSP_PNR string,
        PS_POSID string,
        PS_PSPID string,
        FKART string,
        VKORG string,
        VTWEG string,
        SPART string,
        MATNR_COPA string,
        MATKL string,
        KDGRP string,
        LAND1 string,
        BRSCH string,
        BZIRK string,
        KUNRE string,
        KUNWE string,
        KONZS string,
        _DATAAGING string,
        DUMMY_CJE_INCL_EEW_PS string,
        BUNNR string,
        DRAFT string,
        RUNID int,
        RUNREFERENCE string,
        ADHOCITEM string,
        ADHOCSET string,
        ADHOCSETITEM string,
        RCODE string,
        ORIG_TYPE string,
        ORIG_REF string,

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
