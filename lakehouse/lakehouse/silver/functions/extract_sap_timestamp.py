# Databricks notebook source
# MAGIC %run ../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE FUNCTION {env_vars.silver_catalog}.ca_cross_application_components.extract_sap_timestamp
        (
            sap_timestamp string
        )
    RETURNS timestamp
    COMMENT "SAP timestamps are 14-digit strings in the format yyyyMMddhhmmss.  This function parses that out and returns a timestamp."
    RETURN
        SELECT
            CASE
                WHEN sap_timestamp = "00000000000000"
                    THEN timestamp('1900-01-01 00:00:00')
                WHEN sap_timestamp = "0E-18"
                    THEN timestamp('1900-01-01 00:00:00')
                WHEN sap_timestamp IS NULL
                    THEN timestamp('1900-01-01 00:00:00')
                ELSE
                    timestamp(
                        concat(substring(sap_timestamp, 1, 4)
                        , '-', substring(sap_timestamp, 5, 2)
                        , '-', substring(sap_timestamp, 7, 2)
                        , ' ', substring(sap_timestamp, 9, 2)
                        , ':', substring(sap_timestamp, 11, 2)
                        , ':', substring(sap_timestamp, 13, 2)
                        )
                    )
            END;
''')

# COMMAND ----------

spark.sql(f'''
    GRANT EXECUTE ON FUNCTION {env_vars.silver_catalog}.ca_cross_application_components.extract_sap_timestamp TO `data-engineers`;
''')