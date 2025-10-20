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

table_name = 'aufk'

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
        AUFNR string,
        AUART string,
        AUTYP string,
        REFNR string,
        ERNAM string,
        ERDAT string,
        AENAM string,
        AEDAT string,
        KTEXT string,
        LTEXT string,
        BUKRS string,
        WERKS string,
        GSBER string,
        KOKRS string,
        CCKEY string,
        KOSTV string,
        STORT string,
        SOWRK string,
        ASTKZ string,
        WAERS string,
        ASTNR string,
        STDAT string,
        ESTNR string,
        PHAS0 string,
        PHAS1 string,
        PHAS2 string,
        PHAS3 string,
        PDAT1 string,
        PDAT2 string,
        PDAT3 string,
        IDAT1 string,
        IDAT2 string,
        IDAT3 string,
        OBJID string,
        VOGRP string,
        LOEKZ string,
        PLGKZ string,
        KVEWE string,
        KAPPL string,
        KALSM string,
        ZSCHL string,
        ABKRS string,
        KSTAR string,
        KOSTL string,
        SAKNR string,
        SETNM string,
        CYCLE string,
        SDATE string,
        SEQNR string,
        USER0 string,
        USER1 string,
        USER2 string,
        USER3 string,
        USER4 decimal(38,18),
        USER5 string,
        USER6 string,
        USER7 string,
        USER8 string,
        USER9 string,
        OBJNR string,
        PRCTR string,
        PSPEL string,
        AWSLS string,
        ABGSL string,
        TXJCD string,
        FUNC_AREA string,
        SCOPE string,
        PLINT string,
        KDAUF string,
        KDPOS string,
        AUFEX string,
        IVPRO string,
        LOGSYSTEM string,
        FLG_MLTPS string,
        ABUKR string,
        AKSTL string,
        SIZECL string,
        IZWEK string,
        UMWKZ string,
        KSTEMPF string,
        ZSCHM string,
        PKOSA string,
        ANFAUFNR string,
        PROCNR string,
        PROTY string,
        RSORD string,
        BEMOT string,
        ADRNRA string,
        ERFZEIT string,
        AEZEIT string,
        CSTG_VRNT string,
        COSTESTNR string,
        VERAA_USER string,
        EEW_AUFK_PS_DUMMY string,
        VNAME string,
        RECID string,
        ETYPE string,
        OTYPE string,
        JV_JIBCL string,
        JV_JIBSA string,
        JV_OCO string,
        CPD_UPDAT decimal(38,18),
        `/CUM/INDCU` string,
        `/CUM/CMNUM` string,
        `/CUM/AUEST` string,
        `/CUM/DESNUM` string,
        AD01PROFNR string,
        VAPLZ string,
        WAWRK string,
        FERC_IND string,
        CLAIM_CONTROL string,
        UPDATE_NEEDED string,
        UPDATE_CONTROL string,
        AUFK_STATUS int,
        OIHANTYP string,
        EB_POST string,
        ORDER_PROC_MODE string,

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

spark.sql(f'''

    SELECT
        c.column_name || ' ' || c.full_data_type || ',' AS column_ddl
    FROM
        vivid_dev_brz.information_schema.columns AS c
    WHERE
        c.table_schema = 'sap_s4hana'
        AND c.table_name = '{table_name}'
    ORDER BY
        c.ordinal_position
''').display()

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
