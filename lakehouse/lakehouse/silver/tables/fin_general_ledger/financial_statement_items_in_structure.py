# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.silver_catalog}.fin_general_ledger.financial_statement_items_in_structure (
        -- key
        client STRING
            COMMENT 'Client identifier',
        financial_statement_version STRING
            COMMENT 'Version of the financial statement',
        sequence_number STRING
            COMMENT 'Sequence number of the item',

        -- payloads    
        node_type STRING
            COMMENT 'Type of the node',
        financial_statement_item STRING
            COMMENT 'Item in the financial statement',
        parent_id STRING
            COMMENT 'Parent identifier',
        child_id STRING
            COMMENT 'Child identifier',
        next_id STRING
            COMMENT 'Next identifier',
        hierarchy_level STRING
            COMMENT 'Level in the hierarchy',
        change_of_sign STRING
            COMMENT 'Change of sign',

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
    )
    CLUSTER BY
        AUTO;
''')