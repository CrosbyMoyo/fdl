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

table_name = '2vf_fi_fxexp_001'

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
        RBUKRS string,
        WSL_GRNI decimal(38,18),
        WSL_AP string,
        WSL_AR string,
        WSL_GL_BORROW decimal(38,18),
        CC_IMPORT decimal(38,18),
        CC_EXPORT decimal(38,18),
        CC_SNP_DATE timestamp,
        Dividends_Payable string,
        RWCUR string,
        ProductPriceNotDelivered decimal(38,18),
        CC_CASH decimal(38,18),
        WSL_GRNI_LOCAL decimal(38,18),
        WSL_AR_LOCAL decimal(38,18),
        CC_IMPORT_LOCAL decimal(38,18),
        CC_EXPORT_LOCAL decimal(38,18),
        WSL_AP_LOCAL decimal(38,18),
        CC_OVERDRAFT decimal(38,18),
        DOC_CURR string,
        CC_SOURCE_SYSTEM string,
        CC_WEEK int,
        PRND_S4 string,
        CC_R_EXPORT string,
        CC_R_GRNI string,
        CC_R_IMPORT string,
        CC_R_AP string,
        CC_R_AR string,

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
