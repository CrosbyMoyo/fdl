# Databricks notebook source
# MAGIC %run ../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE FUNCTION {env_vars.silver_catalog}.ca_cross_application_components.extract_sap_date
        (
            sap_date string
        )
    RETURNS date
    COMMENT "SAP dates are 8-digit strings in the format yyyyMMdd.  This function parses that out and returns a date."
    RETURN
        SELECT
            CASE
                WHEN sap_date = "00000000"
                    THEN NULL
                ELSE
                    date(
                        concat(substring(sap_date, 1, 4)
                        , '-', substring(sap_date, 5, 2)
                        , '-', substring(sap_date, 7, 2)
                        )
                    )
            END;
''')

# COMMAND ----------

spark.sql(f'''
    GRANT EXECUTE ON FUNCTION {env_vars.silver_catalog}.ca_cross_application_components.extract_sap_date TO `data-engineers`;
''')