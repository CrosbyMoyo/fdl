# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.ca_cross_application_components_presentation.dim_company_code
    AS
    SELECT
        dcc.company_code_skey
        ,dcc.company_code
        ,dcc.company_name
        ,dcc.country_key
        ,dcc.city
        ,dcc.currency_skey
        ,dcc.chart_of_accounts
        ,dcc.credit_control_area
        ,dcc.display_name
        ,dcc.reporting_entity
        ,dcc.geo_region
        ,dcc.vivo_group
        ,dcc.entity_grouping_level_top
        ,dcc.entity_grouping_level_0
        ,dcc.entity_grouping_level_1
        ,dcc.operating_unit
        ,dcc.entity_grouping_level_2_geographical
        ,dcc.entity_grouping_level_3_vp_reporting
        ,dcc.region_alternative_2
        ,dcc.planning_company_code
        ,dcc.central_credit_country_grouping
        ,dcc.reporting_entity_ri
    FROM
        {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code AS dcc;

''')

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'''
        GRANT ALL PRIVILEGES
        ON VIEW {env_vars.gold_catalog}.ca_cross_application_components_presentation.dim_company_code
        TO `data-engineers`;
    ''')