# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.sl_extended_warehouse_management_presentation.dim_material
    AS
    SELECT
        m.material_skey
        ,m.material_number
        ,m.material_type
        ,m.industry_sector
        ,m.material_group
        ,m.base_unit_of_measure
        ,m.labor_office
        ,m.volume
        ,m.division
        ,m.length
        ,m.product_hierarchy
        ,m.external_material_group
        ,m.transportation_group
        ,m.manufacturer_book_part_number
    FROM
        {env_vars.gold_catalog}.sl_extended_warehouse_management.dim_material AS m;

''')

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'''
        GRANT ALL PRIVILEGES
        ON VIEW {env_vars.gold_catalog}.sl_extended_warehouse_management_presentation.dim_material
        TO `data-engineers`;
    ''')