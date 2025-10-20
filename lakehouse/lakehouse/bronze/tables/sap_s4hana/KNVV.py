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

table_name = 'knvv'

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
        KUNNR string,
        VKORG string,
        VTWEG string,
        SPART string,
        ERNAM string,
        ERDAT string,
        BEGRU string,
        LOEVM string,
        VERSG string,
        AUFSD string,
        KALKS string,
        KDGRP string,
        BZIRK string,
        KONDA string,
        PLTYP string,
        AWAHR string,
        INCO1 string,
        INCO2 string,
        LIFSD string,
        AUTLF string,
        ANTLF decimal(38,18),
        KZTLF string,
        KZAZU string,
        CHSPL string,
        LPRIO string,
        EIKTO string,
        VSBED string,
        FAKSD string,
        MRNKZ string,
        PERFK string,
        PERRL string,
        KVAKZ string,
        KVAWT decimal(38,18),
        WAERS string,
        KLABC string,
        KTGRD string,
        ZTERM string,
        VWERK string,
        VKGRP string,
        VKBUR string,
        VSORT string,
        KVGR1 string,
        KVGR2 string,
        KVGR3 string,
        KVGR4 string,
        KVGR5 string,
        BOKRE string,
        BOIDT string,
        KURST string,
        PRFRE string,
        PRAT1 string,
        PRAT2 string,
        PRAT3 string,
        PRAT4 string,
        PRAT5 string,
        PRAT6 string,
        PRAT7 string,
        PRAT8 string,
        PRAT9 string,
        PRATA string,
        KABSS string,
        KKBER string,
        CASSD string,
        RDOFF string,
        AGREL string,
        MEGRU string,
        UEBTO decimal(38,18),
        UNTTO decimal(38,18),
        UEBTK string,
        PVKSM string,
        PODKZ string,
        PODTG decimal(38,18),
        BLIND string,
        CARRIER_NOTIF string,
        CVP_XBLCK_V string,
        INCOV string,
        INCO2_L string,
        INCO3_L string,
        KNVV_EEW_CONTACT string,
        `/BEV1/EMLGPFAND` string,
        `/BEV1/EMLGFORTS` string,
        `/ICO/KZ_AB` string,
        `/ICO/KZ_SR` string,
        `/ICO/KZ_TR` string,
        `/ICO/LSPKZ` string,
        `/ICO/FAX` string,
        `/ICO/FWERKS` string,
        `/ICO/MAIL` string,
        J_1NBOESL string,
        FSH_KVGR6 string,
        FSH_KVGR7 string,
        FSH_KVGR8 string,
        FSH_KVGR9 string,
        FSH_KVGR10 string,
        FSH_GRREG string,
        FSH_RESGY string,
        FSH_SC_CID string,
        FSH_VAS_DETC string,
        FSH_VAS_CG string,
        FSH_GRSGY string,
        FSH_SS string,
        FSH_FRATE string,
        FSH_FRATE_AGG_LEVEL string,
        FSH_MSOCDC string,
        FSH_MSOPID string,
        RFM_PSST_RULE string,
        OIC_MOT string,
        OILASTOR string,
        OIC_HOBIND string,
        OIABTNR string,
        OIPAFKT string,
        OIGROUPNAM string,
        OIHANTYP string,
        OIINEX string,
        OIPFLIC string,
        OIHCGROUP string,
        STATUS_OBJ_GUID binary,
        BILLPLAN_PROC string,
        RFM_PSST_EXCLUDE string,
        INCO2_KEY binary,
        INCO3_KEY binary,
        INCO4_KEY binary,
        KNVV_ADDR_EEW_CUST string,

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
