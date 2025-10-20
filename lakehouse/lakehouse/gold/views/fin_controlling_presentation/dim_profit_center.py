# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_controlling_presentation.dim_profit_center
    AS
    SELECT
        dpc.profit_center_skey
        ,dpc.profit_center
        ,dpc.profit_center_description
        ,dpc.controlling_area
        ,dpc.segment
        ,dpc.segment_description
        ,dpc.line_of_business
        ,dpc.line_of_business_description
        ,dpc.line_of_business_1
        ,dpc.line_of_business_1_description
        ,dpc.volume_flag_ind
        ,dpc.sales_organization
        ,dpc.sales_organization_description
        ,dpc.distribution_channel
        ,dpc.distribution_channel_description
        ,dpc.division
        ,dpc.division_description
    FROM
        {env_vars.gold_catalog}.fin_controlling.dim_profit_center AS dpc;
''')

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'''
        GRANT ALL PRIVILEGES
        ON VIEW {env_vars.gold_catalog}.fin_controlling_presentation.dim_profit_center
        TO `data-engineers`;
    ''')