# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.silver_catalog}.sl_extended_warehouse_management.material_master (
        --key
        client STRING
            COMMENT 'Client',
        material_number STRING
            COMMENT 'Material',

        -- payload
        material_type STRING
            COMMENT 'Material Type',
        industry_sector STRING
            COMMENT 'Industry Sector',
        material_group STRING
            COMMENT 'Material Group',
        base_unit_of_measure STRING
            COMMENT 'Base Unit of Measure',
        labor_office STRING
            COMMENT 'Labor Office',
        volume DECIMAL(28,4)
            COMMENT 'Volume',
        transportation_group STRING
            COMMENT 'Transportation Group',
        division STRING
            COMMENT 'Division',
        length DECIMAL(28,4)
            COMMENT 'Length',
        product_hierarchy STRING
            COMMENT 'Product Hierarchy',
        external_material_group STRING
            COMMENT 'External Material Group',
        manufacturer_book_part_number STRING
            COMMENT 'Manufacturer Book Part Number',
        ranking INT
            COMMENT 'Ranking',
    
        -- metadata
        __etl_keys_fprint BIGINT
            COMMENT 'xxhash64 of the Business Keys that this record is made up of (or, where the table is the result of joins, it is the fields that make the record unique)',
        __etl_row_fprint BIGINT
            COMMENT ' the xxhash64 of all the columns that make up the row payload (i.e. all the non-key, and non-metadata columns). Note: all columns must be NOT NULL for the hash to calculate properly.',
        __etl_effective_from DATE
            COMMENT 'date (as DATE datatype) that row is effective from. For an updated record this is the previous _effective_to date + 1 day.',
        __etl_effective_to DATE
            COMMENT 'date (as DATE datatype) that row is effective to, or NULL for active record',
        __etl_is_active BOOLEAN
            COMMENT 'boolean flag indicating the active record. Note: there should only be 1 _is_active for any _etl_keys_fprint',
        __etl_is_deleted BOOLEAN
            COMMENT 'boolean showing if the record has been deleted from the source system'
        );
''')