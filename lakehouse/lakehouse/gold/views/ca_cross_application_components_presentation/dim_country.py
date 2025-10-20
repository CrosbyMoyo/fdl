# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.ca_cross_application_components_presentation.dim_country
    AS
    SELECT
        dc.country_key
        ,dc.country_name_short
        ,dc.country_name_full
        ,dc.longitude
        ,dc.latitude
    FROM
        {env_vars.gold_catalog}.ca_cross_application_components.dim_country AS dc;

''')

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'''
        GRANT ALL PRIVILEGES
        ON VIEW {env_vars.gold_catalog}.ca_cross_application_components_presentation.dim_country
        TO `data-engineers`;
    ''')