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

table_name = 'mvke'

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
        VKORG string,
        VTWEG string,
        LVORM string,
        VERSG string,
        BONUS string,
        PROVG string,
        SKTOF string,
        VMSTA string,
        VMSTD string,
        AUMNG decimal(38,18),
        LFMNG decimal(38,18),
        EFMNG decimal(38,18),
        SCMNG decimal(38,18),
        SCHME string,
        VRKME string,
        MTPOS string,
        DWERK string,
        PRODH string,
        PMATN string,
        KONDM string,
        KTGRM string,
        MVGR1 string,
        MVGR2 string,
        MVGR3 string,
        MVGR4 string,
        MVGR5 string,
        SSTUF string,
        PFLKS string,
        LSTFL string,
        LSTVZ string,
        LSTAK string,
        LDVFL string,
        LDBFL string,
        LDVZL string,
        LDBZL string,
        VDVFL string,
        VDBFL string,
        VDVZL string,
        VDBZL string,
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
        RDPRF string,
        MEGRU string,
        LFMAX decimal(38,18),
        RJART string,
        PBIND string,
        VAVME string,
        MATKC string,
        PVMSO string,
        DUMMY_SALD_INCL_EEW_PS string,
        `/BEV1/EMLGRP` string,
        `/BEV1/EMDRCKSPL` string,
        `/BEV1/RPBEZME` string,
        `/BEV1/RPSNS` string,
        `/BEV1/RPSFA` string,
        `/BEV1/RPSKI` string,
        `/BEV1/RPSCO` string,
        `/BEV1/RPSSO` string,
        NF_FLAG string,
        PLGTP string,
        `/ICO/MDMM` string,
        CTR_TERM_DEF string,
        CTR_TERM_ALT1 string,
        CTR_TERM_ALT2 string,
        CTR_TERM_UNIT string,
        EXT_PERIOD_DEF string,
        EXT_PERIOD_ALT1 string,
        EXT_PERIOD_ALT2 string,
        EXT_PERIOD_UNIT string,
        IS_ENTLMNT_RLVT string,
        PACKAGE_TYPE string,
        PACKAGE_SIZE string,

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
