# Databricks notebook source
# MAGIC %run ../../common/properties

# COMMAND ----------

spark.sql(f'''
    -- transfrom the sap indicators to booleans
    CREATE OR REPLACE FUNCTION {env_vars.silver_catalog}.ca_cross_application_components.cast_sap_indicator(indicator STRING)
    RETURNS BOOLEAN
    RETURN 
        CASE 
            WHEN indicator = 'X' THEN TRUE 
            ELSE FALSE 
        END;
''')

# COMMAND ----------

spark.sql(f'''
    GRANT EXECUTE ON FUNCTION {env_vars.silver_catalog}.ca_cross_application_components.cast_sap_indicator TO `data-engineers`;
''')