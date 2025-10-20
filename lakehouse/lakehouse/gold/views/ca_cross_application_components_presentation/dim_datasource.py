# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.ca_cross_application_components_presentation.dim_datasource
    AS
    SELECT
        ds.datasource_skey
        ,ds.datasource_name
        ,ds.datasource_description
    FROM
        {env_vars.gold_catalog}.ca_cross_application_components.dim_datasource AS ds;

''')

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'''
        GRANT ALL PRIVILEGES
        ON VIEW {env_vars.gold_catalog}.ca_cross_application_components_presentation.dim_datasource
        TO `data-engineers`;
    ''')