# Databricks notebook source
spark.sql('''
 -- transform description by removing special characters except underscores and replacing spaces with underscores
    CREATE OR REPLACE FUNCTION vivid_meta.vivid_meta.get_derived_name(name_column STRING)
    RETURNS STRING
    RETURN 
        REGEXP_REPLACE(
            LOWER(REGEXP_REPLACE(name_column, '[^a-zA-Z0-9_ ]', '')),
            ' ', '_' 
        );
''')

# COMMAND ----------

spark.sql('''
    -- transfrom the sap indicators to booleans
    CREATE OR REPLACE FUNCTION vivid_meta.vivid_meta.convert_sap_indicator_to_boolean(indicator STRING)
    RETURNS BOOLEAN
    RETURN 
        CASE 
            WHEN indicator = 'X' THEN TRUE 
            ELSE FALSE 
        END;
''')