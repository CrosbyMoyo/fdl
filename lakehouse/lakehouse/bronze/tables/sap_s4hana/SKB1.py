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

table_name = 'skb1'

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
        SAKNR string,
        BEGRU string,
        BUSAB string,
        DATLZ string,
        ERDAT string,
        ERNAM string,
        FDGRV string,
        FDLEV string,
        FIPLS string,
        FSTAG string,
        HBKID string,
        HKTID string,
        KDFSL string,
        MITKZ string,
        MWSKZ string,
        STEXT string,
        VZSKZ string,
        WAERS string,
        WMETH string,
        XGKON string,
        XINTB string,
        XKRES string,
        XLOEB string,
        XNKON string,
        XOPVW string,
        XSPEB string,
        ZINDT string,
        ZINRT string,
        ZUAWA string,
        ALTKT string,
        XMITK string,
        RECID string,
        FIPOS string,
        XMWNO string,
        XSALH string,
        BEWGP string,
        INFKY string,
        TOGRU string,
        XLGCLR string,
        MCAKEY string,
        COCHANGED string,
        LAST_CHANGED_TS decimal(38,18),
        X_UJ_CLR string,

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
