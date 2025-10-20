# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.ca_cross_application_components_presentation.dim_version
    AS
    SELECT
        dv.version_skey
        ,dv.version_id
        ,dv.version_description
    FROM
        {env_vars.gold_catalog}.ca_cross_application_components.dim_version AS dv;

''')

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'''
        GRANT ALL PRIVILEGES
        ON VIEW {env_vars.gold_catalog}.ca_cross_application_components_presentation.dim_version
        TO `data-engineers`;
    ''')