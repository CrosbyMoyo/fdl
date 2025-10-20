# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

# create table dim_company_code
spark.sql(f'''
  CREATE OR REPLACE TABLE {env_vars.gold_catalog}.sl_extended_warehouse_management.dim_material
  (
    -- Keys
    material_skey BIGINT NOT NULL
      COMMENT 'DWH generated identifier for material',
    material_number STRING
      COMMENT 'Unique identifier for material',

    -- Payload
    material_type STRING 
      COMMENT 'Type of material',
    industry_sector STRING 
      COMMENT 'Industry sector of the material',
    material_group STRING 
      COMMENT 'Group classification of the material',
    base_unit_of_measure STRING
      COMMENT 'Base unit of measure for the material',
    labor_office STRING 
      COMMENT 'Labor office associated with the material',
    volume STRING
      COMMENT 'Volume of the material',
    division STRING
      COMMENT 'Division associated with the material',
    length STRING
      COMMENT 'Length of the material',
    product_hierarchy STRING
      COMMENT 'Product hierarchy classification',
    external_material_group STRING
      COMMENT 'External group classification of the material',
    transportation_group STRING
      COMMENT 'Transportation group for the material',
    manufacturer_book_part_number STRING
      COMMENT 'Manufacturer book part number for the material',

    -- Metadata
    __etl_keys_fprint BIGINT
      COMMENT "xxhash64 of the Business Keys that this record is made up of",
    __etl_row_fprint BIGINT
      COMMENT "the xxhash64 of all the columns that make up the row payload",
    __etl_effective_from DATE
      COMMENT "Date that row is effective from",
    __etl_effective_to DATE
      COMMENT "Date that row is effective to, or NULL for active record",
    __etl_is_active BOOLEAN
      COMMENT "flag indicating the active record. Note: there should only be 1 _is_active for any _etl_keys_fprint",
    __etl_is_deleted BOOLEAN
      COMMENT "showing if the record has been deleted from the source system",

    -- PK
    CONSTRAINT pk_dim_material PRIMARY KEY (material_skey)
  )
  COMMENT 'Material details from S4HANA.'
  CLUSTER BY
    AUTO;
''')
