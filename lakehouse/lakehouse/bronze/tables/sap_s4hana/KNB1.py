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

table_name = 'knb1'

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
        BUKRS string,
        PERNR string,
        KNB1_EEW_CC string,
        ERDAT string,
        ERNAM string,
        SPERR string,
        LOEVM string,
        ZUAWA string,
        BUSAB string,
        AKONT string,
        BEGRU string,
        KNRZE string,
        KNRZB string,
        ZAMIM string,
        ZAMIV string,
        ZAMIR string,
        ZAMIB string,
        ZAMIO string,
        ZWELS string,
        XVERR string,
        ZAHLS string,
        ZTERM string,
        WAKON string,
        VZSKZ string,
        ZINDT string,
        ZINRT string,
        EIKTO string,
        ZSABE string,
        KVERM string,
        FDGRV string,
        VRBKZ string,
        VLIBB decimal(38,18),
        VRSZL decimal(38,18),
        VRSPR decimal(38,18),
        VRSNR string,
        VERDT string,
        PERKZ string,
        XDEZV string,
        XAUSZ string,
        WEBTR decimal(38,18),
        REMIT string,
        DATLZ string,
        XZVER string,
        TOGRU string,
        KULTG decimal(38,18),
        HBKID string,
        XPORE string,
        BLNKZ string,
        ALTKN string,
        ZGRUP string,
        URLID string,
        MGRUP string,
        LOCKB string,
        UZAWE string,
        EKVBD string,
        SREGL string,
        XEDIP string,
        FRGRP string,
        VRSDG string,
        TLFXS string,
        INTAD string,
        XKNZB string,
        GUZTE string,
        GRICD string,
        GRIDT string,
        WBRSL string,
        CONFS string,
        UPDAT string,
        UPTIM string,
        NODEL string,
        TLFNS string,
        CESSION_KZ string,
        AVSND string,
        AD_HASH string,
        QLAND string,
        CVP_XBLCK_B string,
        CIIUCODE string,
        GMVKZD string,
        INTERCOCD string,
        PAYMENTCLEARINGGRPID string,
        PAYTRSN string,

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
