# Databricks notebook source
# TODO: is this table used/needed?

# COMMAND ----------

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

table_name = 'cosp'

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
        LEDNR string,
        OBJNR string,
        GJAHR string,
        WRTTP string,
        VERSN string,
        KSTAR string,
        HRKFT string,
        VRGNG string,
        VBUND string,
        PARGB string,
        BEKNZ string,
        TWAER string,
        PERBL string,
        MEINH string,
        WTG001 decimal(38,18),
        WTG002 decimal(38,18),
        WTG003 decimal(38,18),
        WTG004 decimal(38,18),
        WTG005 decimal(38,18),
        WTG006 decimal(38,18),
        WTG007 decimal(38,18),
        WTG008 decimal(38,18),
        WTG009 decimal(38,18),
        WTG010 decimal(38,18),
        WTG011 decimal(38,18),
        WTG012 decimal(38,18),
        WTG013 decimal(38,18),
        WTG014 decimal(38,18),
        WTG015 decimal(38,18),
        WTG016 decimal(38,18),
        WOG001 decimal(38,18),
        WOG002 decimal(38,18),
        WOG003 decimal(38,18),
        WOG004 decimal(38,18),
        WOG005 decimal(38,18),
        WOG006 decimal(38,18),
        WOG007 decimal(38,18),
        WOG008 decimal(38,18),
        WOG009 decimal(38,18),
        WOG010 decimal(38,18),
        WOG011 decimal(38,18),
        WOG012 decimal(38,18),
        WOG013 decimal(38,18),
        WOG014 decimal(38,18),
        WOG015 decimal(38,18),
        WOG016 decimal(38,18),
        WKG001 decimal(38,18),
        WKG002 decimal(38,18),
        WKG003 decimal(38,18),
        WKG004 decimal(38,18),
        WKG005 decimal(38,18),
        WKG006 decimal(38,18),
        WKG007 decimal(38,18),
        WKG008 decimal(38,18),
        WKG009 decimal(38,18),
        WKG010 decimal(38,18),
        WKG011 decimal(38,18),
        WKG012 decimal(38,18),
        WKG013 decimal(38,18),
        WKG014 decimal(38,18),
        WKG015 decimal(38,18),
        WKG016 decimal(38,18),
        WKF001 decimal(38,18),
        WKF002 decimal(38,18),
        WKF003 decimal(38,18),
        WKF004 decimal(38,18),
        WKF005 decimal(38,18),
        WKF006 decimal(38,18),
        WKF007 decimal(38,18),
        WKF008 decimal(38,18),
        WKF009 decimal(38,18),
        WKF010 decimal(38,18),
        WKF011 decimal(38,18),
        WKF012 decimal(38,18),
        WKF013 decimal(38,18),
        WKF014 decimal(38,18),
        WKF015 decimal(38,18),
        WKF016 decimal(38,18),
        PAG001 decimal(38,18),
        PAG002 decimal(38,18),
        PAG003 decimal(38,18),
        PAG004 decimal(38,18),
        PAG005 decimal(38,18),
        PAG006 decimal(38,18),
        PAG007 decimal(38,18),
        PAG008 decimal(38,18),
        PAG009 decimal(38,18),
        PAG010 decimal(38,18),
        PAG011 decimal(38,18),
        PAG012 decimal(38,18),
        PAG013 decimal(38,18),
        PAG014 decimal(38,18),
        PAG015 decimal(38,18),
        PAG016 decimal(38,18),
        MEG001 decimal(38,18),
        MEG002 decimal(38,18),
        MEG003 decimal(38,18),
        MEG004 decimal(38,18),
        MEG005 decimal(38,18),
        MEG006 decimal(38,18),
        MEG007 decimal(38,18),
        MEG008 decimal(38,18),
        MEG009 decimal(38,18),
        MEG010 decimal(38,18),
        MEG011 decimal(38,18),
        MEG012 decimal(38,18),
        MEG013 decimal(38,18),
        MEG014 decimal(38,18),
        MEG015 decimal(38,18),
        MEG016 decimal(38,18),
        MEF001 decimal(38,18),
        MEF002 decimal(38,18),
        MEF003 decimal(38,18),
        MEF004 decimal(38,18),
        MEF005 decimal(38,18),
        MEF006 decimal(38,18),
        MEF007 decimal(38,18),
        MEF008 decimal(38,18),
        MEF009 decimal(38,18),
        MEF010 decimal(38,18),
        MEF011 decimal(38,18),
        MEF012 decimal(38,18),
        MEF013 decimal(38,18),
        MEF014 decimal(38,18),
        MEF015 decimal(38,18),
        MEF016 decimal(38,18),
        MUV001 string,
        MUV002 string,
        MUV003 string,
        MUV004 string,
        MUV005 string,
        MUV006 string,
        MUV007 string,
        MUV008 string,
        MUV009 string,
        MUV010 string,
        MUV011 string,
        MUV012 string,
        MUV013 string,
        MUV014 string,
        MUV015 string,
        MUV016 string,
        BELTP string,
        TIMESTMP decimal(38,18),
        BUKRS string,
        FKBER string,
        SEGMENT string,
        GEBER string,
        GRANT_NBR string,
        BUDGET_PD string,

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
