# Databricks notebook source
# MAGIC %run ../../common/properties

# COMMAND ----------

spark.sql(f'''
    -- Cast string numeric amounts that may contain commas to decimal 38, 10.
    -- This is a deliberately large scale and to reduce truncating values. Round then cast after this if you need to.
    CREATE OR REPLACE FUNCTION {env_vars.silver_catalog}.ca_cross_application_components.cast_comma_containing_string_amounts(amount STRING)
    RETURNS DECIMAL(38, 10)
    RETURN 
        CAST(REPLACE(amount, ',') AS DECIMAL(38, 10));
''')

# COMMAND ----------

spark.sql(f'''
    GRANT EXECUTE ON FUNCTION {env_vars.silver_catalog}.ca_cross_application_components.cast_comma_containing_string_amounts TO `data-engineers`;
''')