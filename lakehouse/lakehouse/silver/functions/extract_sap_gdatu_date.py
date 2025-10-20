# Databricks notebook source
# MAGIC %run ../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE FUNCTION {env_vars.silver_catalog}.ca_cross_application_components.extract_sap_gdatu_date
        (
            sap_gdatu_date string
        )
    RETURNS date
    COMMENT "SAP GDATU Dates are inverse date formats where instead of 8-digit strings in the format yyyyMMdd the complement of each digit is stored.  This function parses that out and returns a date."
    RETURN
        SELECT
            CASE
                WHEN sap_gdatu_date = "99999999"
                    THEN NULL
                ELSE
                    date(
                        concat(substring(99999999 - int(sap_gdatu_date), 1, 4)
                        , '-', substring(99999999 - int(sap_gdatu_date), 5, 2)
                        , '-', substring(99999999 - int(sap_gdatu_date), 7, 2)
                        )
                    )
            END;
''')

# COMMAND ----------

spark.sql(f'''
    GRANT EXECUTE ON FUNCTION {env_vars.silver_catalog}.ca_cross_application_components.extract_sap_gdatu_date TO `data-engineers`;
''')