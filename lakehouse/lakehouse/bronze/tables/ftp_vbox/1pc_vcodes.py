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

table_name = '1pc_vcodes'

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
        Child_Vcode string,
        Description string,
        Parent_Vcode_H1_REP string,
        Parent_Vcode_H2_BIZ string,
        Parent_Vcode_H3_BS string,
        Parent_Vcode_H4_MIP string,
        Parent_Vcode_H5_BSMIP string,
        C1 string,
        C2 string,
        C3 string,
        C4 string,
        Net_Income string,
        Local_EBITDA string,
        Local_OPEX string,
        OPEX_Type string,
        New_VCode string,
        Direct_Contribution string,
        Indirect_Contribution string,

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

spark.sql(f'''

    SELECT
        c.column_name || ' ' || c.full_data_type || ',' AS column_ddl
    FROM
        vivid_dev_brz.information_schema.columns AS c
    WHERE
        c.table_schema = 'ftp_vbox'
        AND c.table_name = '{table_name}'
    ORDER BY
        c.ordinal_position
''').display()

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
