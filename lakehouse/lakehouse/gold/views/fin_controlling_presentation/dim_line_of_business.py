# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

#TODO: This transformation should be in silver 
spark.sql(
    f'''
        CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_controlling_presentation.dim_line_of_business
        AS
        SELECT
            dlob.line_of_business_skey
            ,dlob.line_of_business
            ,dlob.line_of_business_description
            ,dlob.class_of_business
            ,dlob.class_of_business_description
        FROM
            {env_vars.gold_catalog}.fin_controlling.dim_line_of_business AS dlob;
'''
)

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'''
        GRANT ALL PRIVILEGES
        ON VIEW {env_vars.gold_catalog}.fin_controlling_presentation.dim_line_of_business
        TO `data-engineers`;
    ''')