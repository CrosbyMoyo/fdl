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

table_name = 'lfb1'

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
        LIFNR string,
        BUKRS string,
        PERNR string,
        ERDAT string,
        ERNAM string,
        SPERR string,
        LOEVM string,
        ZUAWA string,
        AKONT string,
        BEGRU string,
        VZSKZ string,
        ZWELS string,
        XVERR string,
        ZAHLS string,
        ZTERM string,
        EIKTO string,
        ZSABE string,
        KVERM string,
        FDGRV string,
        BUSAB string,
        LNRZE string,
        LNRZB string,
        ZINDT string,
        ZINRT string,
        DATLZ string,
        XDEZV string,
        WEBTR decimal(38,18),
        KULTG decimal(38,18),
        REPRF string,
        TOGRU string,
        HBKID string,
        XPORE string,
        QSZNR string,
        QSZDT string,
        QSSKZ string,
        BLNKZ string,
        MINDK string,
        ALTKN string,
        ZGRUP string,
        MGRUP string,
        UZAWE string,
        QSREC string,
        QSBGR string,
        QLAND string,
        XEDIP string,
        FRGRP string,
        TOGRR string,
        TLFXS string,
        INTAD string,
        XLFZB string,
        GUZTE string,
        GRICD string,
        GRIDT string,
        XAUSZ string,
        CERDT string,
        CONFS string,
        UPDAT string,
        UPTIM string,
        NODEL string,
        TLFNS string,
        AVSND string,
        AD_HASH string,
        CVP_XBLCK_B string,
        CIIUCODE string,
        LFB1_EEW_CC string,
        ZBOKD string,
        ZQSSKZ string,
        ZQSZDT string,
        ZQSZNR string,
        ZMINDAT string,
        J_SC_SUBCONTYPE string,
        J_SC_COMPDATE string,
        J_SC_OFFSM string,
        J_SC_OFFSR string,
        BASIS_PNT decimal(38,18),
        GMVKZK string,
        BRSCH string,
        WRBTR decimal(38,18),
        FORGN string,
        SHARE_IN_FOREIGN decimal(38,18),
        NOTES string,
        ACTIVE string,
        INTERCOCD string,
        RSTR_CHG_FL string,
        CHECK_FLAG string,
        OVRD_RCPMT string,
        MIN_PAY decimal(38,18),
        PAY_FRQ_CD string,
        RECOUP_PC decimal(38,18),
        ALLOT_MTH_CD string,
        ESCH_CD string,
        ESCHEAT_DT string,
        PREPAY_RELEVANT string,
        ASSIGN_TEST string,
        WAERS string,
        US_REC_COUNTRY string,
        US_GIIN string,
        US_FTID string,
        US_REC_DOB string,
        US_LOB_CODE string,
        US_W8_RECVDATE string,
        US_W9_RECVDATE string,
        US_TIN_NOTICE string,
        US_PARTNERSHIP_IND string,
        PAYMENTCLEARINGGRPID string,
        PAYTRSN string,
        US_FATCA_IND string,
        US_CHAP4_STATUS_CODE string,

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
