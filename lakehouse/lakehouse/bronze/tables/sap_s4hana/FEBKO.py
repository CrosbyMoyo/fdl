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

table_name = 'febko'

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
        ANWND string,
        ABSND string,
        AZIDT string,
        EMKEY string,
        KUKEY string,
        ASTAT string,
        DSTAT string,
        VB1OK string,
        VB2OK string,
        KIPRE string,
        VFDAT string,
        GRP01 string,
        XKEP1 string,
        GRP02 string,
        XKEP2 string,
        WVDAT string,
        WVTIM string,
        WVART string,
        HKONT string,
        KTONR string,
        KTOIH string,
        KTOSB string,
        AZNUM string,
        AZSNR string,
        AZDAT string,
        BUKRS string,
        KTOPL string,
        WAERS string,
        SSTYP string,
        SSVOZ string,
        SSBTR decimal(38,18),
        SUMSO decimal(38,18),
        SUMHA decimal(38,18),
        ESTYP string,
        ESVOZ string,
        ESBTR decimal(38,18),
        ESDMB decimal(38,18),
        BLAUF string,
        ELAUF string,
        HZINS decimal(38,18),
        TEILN string,
        BKTOA string,
        ANZES string,
        VGTYP string,
        EFART string,
        HBKID string,
        HKTID string,
        EUSER string,
        EDATE string,
        ETIME string,
        BKREF string,
        XFDIS string,
        DSART string,
        XVERD string,
        XBENR string,
        XBTYP string,
        SEQ_NUMBER string,
        SEQ_STATUS string,
        INPUT_FORMAT string,
        SIBAN string,
        AZPGNO string,
        DUMMY_FEBKO string,
        FILEHASH string,
        CLOSING_AVAILABLE_BALANCE decimal(38,18),
        BSIMP_PSETID string,
        SUPFINCOR string,
        CASHUPD string,
        CASHUPDOK string,
        FILE_FORMAT string,
        FILE_FORMAT_MAPPING string,

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
