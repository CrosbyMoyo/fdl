# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.gold_catalog}.fin_general_ledger.dim_vcodes
    (
        vcode_skey BIGINT
            CONSTRAINT dim_vcodes__pk PRIMARY KEY
            COMMENT 'SKey for the Vcode',
        vcode STRING 
            COMMENT 'GL Account identifier',
        description STRING
            COMMENT 'Description of the GL Account',
        c1_relevant_flag BOOLEAN
            COMMENT 'Flag indicating if Vcode is relevant for C1',
        c2_relevant_flag BOOLEAN
            COMMENT 'Flag indicating if Vcode is relevant for C2',
        c3_relevant_flag BOOLEAN
            COMMENT 'Flag indicating if Vcode is relevant for C3',
        c4_relevant_flag BOOLEAN
            COMMENT 'Flag indicating if Vcode is relevant for C4',
        net_income_relevant_flag BOOLEAN
            COMMENT 'Flag indicating if Vcode is relevant for Net Income',
        local_ebitda_relevant_flag BOOLEAN
            COMMENT 'Flag indicating if Vcode is relevant for Local EBITDA',
        local_opex STRING
            COMMENT 'Local Opex',
        opex_description STRING
            COMMENT 'Description of the Opex',
        opex_type STRING
            COMMENT 'Type of Opex',
        opex_type_description STRING
            COMMENT 'Description of the Opex Type',
        direct_contribution_relevant_flag BOOLEAN
            COMMENT 'Flag indicating if Vcode is relevant for Direct Contribution',
        indirect_contribution_relevant_flag BOOLEAN
            COMMENT 'Flag indicating if Vcode is relevant for Indirect Contribution',
        vcode_sort_order INT
            COMMENT 'Sort order for Vcode',

        -- Metadata columns
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
        COMMENT "showing if the record has been deleted from the source system"
    )
    CLUSTER BY
        AUTO;
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE {env_vars.gold_catalog}.fin_general_ledger.dim_vcodes
    SET TAGS ('source_system' = 'ftp_vbox', 'source_table' = '1pc_vcodes');
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE {env_vars.gold_catalog}.fin_general_ledger.dim_vcodes
    ALTER COLUMN vcode
    SET TAGS ('source_system' = 'ftp_vbox', 'source_table' = '1pc_vcodes', 'source_field' = 'Child_Vcode');
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE {env_vars.gold_catalog}.fin_general_ledger.dim_vcodes
    ALTER COLUMN description
    SET TAGS ('source_system' = 'ftp_vbox', 'source_table' = '1pc_vcodes', 'source_field' = 'Description');
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE {env_vars.gold_catalog}.fin_general_ledger.dim_vcodes
    ALTER COLUMN c1_relevant_flag
    SET TAGS ('source_system' = 'ftp_vbox', 'source_table' = '1pc_vcodes', 'source_field' = 'C1');
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE {env_vars.gold_catalog}.fin_general_ledger.dim_vcodes
    ALTER COLUMN c2_relevant_flag
    SET TAGS ('source_system' = 'ftp_vbox', 'source_table' = '1pc_vcodes', 'source_field' = 'C2');
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE {env_vars.gold_catalog}.fin_general_ledger.dim_vcodes
    ALTER COLUMN c3_relevant_flag
    SET TAGS ('source_system' = 'ftp_vbox', 'source_table' = '1pc_vcodes', 'source_field' = 'C3');
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE {env_vars.gold_catalog}.fin_general_ledger.dim_vcodes
    ALTER COLUMN c4_relevant_flag
    SET TAGS ('source_system' = 'ftp_vbox', 'source_table' = '1pc_vcodes', 'source_field' = 'C4');
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE {env_vars.gold_catalog}.fin_general_ledger.dim_vcodes
    ALTER COLUMN net_income_relevant_flag
    SET TAGS ('source_system' = 'ftp_vbox', 'source_table' = '1pc_vcodes', 'source_field' = 'Net_Income');
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE {env_vars.gold_catalog}.fin_general_ledger.dim_vcodes
    ALTER COLUMN local_ebitda_relevant_flag
    SET TAGS ('source_system' = 'ftp_vbox', 'source_table' = '1pc_vcodes', 'source_field' = 'Local_EBITDA');
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE {env_vars.gold_catalog}.fin_general_ledger.dim_vcodes
    ALTER COLUMN local_opex
    SET TAGS ('source_system' = 'ftp_vbox', 'source_table' = '1pc_vcodes', 'source_field' = 'Local_OPEX');
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE {env_vars.gold_catalog}.fin_general_ledger.dim_vcodes
    ALTER COLUMN opex_description
    SET TAGS ('source_system' = 'ftp_vbox', 'source_table' = '1pc_vcodes', 'source_field' = 'If local opex has value "C", then "Central Opex" if "L", then "Local Opex", otherwise null');
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE {env_vars.gold_catalog}.fin_general_ledger.dim_vcodes
    ALTER COLUMN opex_type
    SET TAGS ('source_system' = 'ftp_vbox', 'source_table' = '1pc_vcodes', 'source_field' = 'OPEX_type');
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE {env_vars.gold_catalog}.fin_general_ledger.dim_vcodes
    ALTER COLUMN opex_type_description
    SET TAGS ('source_system' = 'ftp_vbox', 'source_table' = '1pc_vcodes', 'source_field' = 'If opex_type has value "V" then "Variable Opex" if "F" then "Fixed Opex", otherwise null');
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE {env_vars.gold_catalog}.fin_general_ledger.dim_vcodes
    ALTER COLUMN direct_contribution_relevant_flag
    SET TAGS ('source_system' = 'ftp_vbox', 'source_table' = '1pc_vcodes', 'source_field' = 'Direct_Contribution');
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE {env_vars.gold_catalog}.fin_general_ledger.dim_vcodes
    ALTER COLUMN indirect_contribution_relevant_flag
    SET TAGS ('source_system' = 'ftp_vbox', 'source_table' = '1pc_vcodes', 'source_field' = 'Indirect_Contribution');
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE {env_vars.gold_catalog}.fin_general_ledger.dim_vcodes
    ALTER COLUMN vcode_sort_order
    SET TAGS ('source_system' = 'ftp_vbox', 'source_table' = '1pc_vcodes', 'source_field' = 'New_Vcode');
''')