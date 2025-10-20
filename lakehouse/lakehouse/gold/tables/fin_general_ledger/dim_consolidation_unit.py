# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.gold_catalog}.fin_general_ledger.dim_consolidation_unit (
        --key
        consolidation_unit_skey BIGINT
            COMMENT 'Consolidation Unit Skey',
        dimension STRING
            COMMENT 'Dimension',
        consolidation_unit STRING
            COMMENT 'Consolidation Unit',
        
        -- payload
        consolidation_unit_description STRING
            COMMENT 'Consolidation Unit Description',
        consolidation_group STRING
            COMMENT 'Consolidation Group',
        country_region STRING
            COMMENT 'Country Region',
        company STRING
            COMMENT 'Company',

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
            COMMENT 'boolean showing if the record has been deleted from the source system',
    CONSTRAINT pk_dim_consolidation_unit PRIMARY KEY (consolidation_unit_skey)
    )
    CLUSTER BY
        AUTO;
''')