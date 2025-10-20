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

table_name = '2vf_fi_reporting_001'

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
        RLDNR string,
        COArea string,
        Chart_of_Account string,
        Company_Code string,
        `Year` int,
        Period int,
        GL_Account string,
        Cost_Center string,
        Profit_Center string,
        Material string,
        Customer string,
        VCode string,
        RITEM string,
        DOCTY string,
        PLEVL string,
        RDIMEN string,
        YearPeriod string,
        `Source` string,
        DataSource string,
        SPART string,
        KVGR1_PA string,
        VTWEG string,
        VKORG string,
        KDGRP string,
        ABTEI string,
        Billto_Party string,
        Shipto_Party string,
        Amount_LC string,
        BUDAT timestamp,
        Month_End_Date timestamp,
        Amount_GC string,
        Reporting_QuantityL20 string,
        Reporting_QuantityLPG string,
        Local_Currency string,
        Volume_Alt string,
        PlanYear bigint,
        LOB1 string,
        Amount_GC_PA decimal(38,18),
        UD1 string,
        LY_Amount_GC_I string,
        UD2 string,
        UD3 string,
        UD4 string,
        UD5 string,
        UD6 string,
        COLL_SPECIALIST string,
        ELIM_Flag string,
        RBUPTR string,
        Last_Refresh_on timestamp,
        IFRS_Flag string,

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
