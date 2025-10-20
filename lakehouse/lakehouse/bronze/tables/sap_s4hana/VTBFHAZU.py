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

table_name = 'vtbfhazu'

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
        RFHA string,
        RFHAZU string,
        CRUSER string,
        DCRDAT string,
        TCRTIM string,
        UPUSER string,
        DUPDAT string,
        TUPTIM string,
        SGSART string,
        SFHAART string,
        SFGZUSTT string,
        SFUNKTV string,
        SFUNKTL string,
        ROFHAZU string,
        RFHAZUX string,
        SAKTIV string,
        SSTOGRD string,
        RDEALER string,
        DVTRAB string,
        TVTRAB string,
        GSPPART string,
        XAKT string,
        NORDEXT string,
        DBLFZ string,
        DELFZ string,
        DFIX string,
        SINCLE string,
        DZSTND string,
        SZNSPRO string,
        DZNSSTD string,
        KKURS decimal(38,18),
        KKASSA decimal(38,18),
        KSWAP decimal(38,18),
        WLWAERS string,
        WFWAERS string,
        LIMITART string,
        LIMITDAT string,
        RKONDGR string,
        LIWAERS string,
        KWLIQUI decimal(38,18),
        SCONFIRM string,
        DEXDAT string,
        UEXNAM string,
        SRECONFIRM string,
        DREDAT string,
        URENAM string,
        DORDER string,
        DANST string,
        TANST string,
        SANST string,
        SSPESEN string,
        BUPRCLIM decimal(38,18),
        SRUNITLIM string,
        BPPRCLIM decimal(38,18),
        JVERK6B string,
        PEFFZINS decimal(38,18),
        PEFFZINS_GIVEN decimal(38,18),
        PEFFZCALL decimal(38,18),
        SEFFMETH string,
        NOTICE_DATE string,
        ROUNDING_RULE string,
        BPPRC_SPOT2 decimal(38,18),
        BPPRC_SPOT1 decimal(38,18),
        BPPRC_MARG decimal(38,18),
        COC_RATE decimal(38,18),
        FORWARD_DATE string,
        SNOTDELIVERED string,
        PEFFZ_WORST decimal(38,18),
        PEFFZ_WORST_DT string,
        ZVTRAB string,
        NOM_UP_LIMIT decimal(38,18),
        NOM_LOW_LIMIT decimal(38,18),
        FIXING_REF_ID string,
        TERMINATION_STRATEGY int,

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
