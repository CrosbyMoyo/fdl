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

table_name = 't005'

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
        LAND1 string,
        LANDK string,
        LNPLZ string,
        PRPLZ string,
        ADDRS string,
        XPLZS string,
        XPLPF string,
        SPRAS string,
        XLAND string,
        XADDR string,
        NMFMT string,
        XREGS string,
        XPLST string,
        INTCA string,
        INTCA3 string,
        INTCN3 string,
        XEGLD string,
        XSKFN string,
        XMWSN string,
        LNBKN string,
        PRBKN string,
        LNBLZ string,
        PRBLZ string,
        LNPSK string,
        PRPSK string,
        XPRBK string,
        BNKEY string,
        LNBKS string,
        PRBKS string,
        XPRSO string,
        PRUIN string,
        UINLN string,
        LNST1 string,
        PRST1 string,
        LNST2 string,
        PRST2 string,
        LNST3 string,
        PRST3 string,
        LNST4 string,
        PRST4 string,
        LNST5 string,
        PRST5 string,
        LANDD string,
        KALSM string,
        LANDA string,
        WECHF string,
        LKVRZ string,
        INTCN string,
        XDEZP string,
        DATFM string,
        CURIN string,
        CURHA string,
        WAERS string,
        KURST string,
        AFAPL string,
        GWGWRT decimal(38,18),
        UMRWRT decimal(38,18),
        KZRBWB string,
        XANZUM string,
        CTNCONCEPT string,
        KZSRV string,
        XXINVE string,
        NET_GROSS_POSTING_TYPE string,
        XGCCV string,
        SUREG string,

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
