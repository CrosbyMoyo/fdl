# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.gold_catalog}.fin_controlling.dim_profit_center
    (
        profit_center_skey BIGINT
            CONSTRAINT dim_profit_center_pk PRIMARY KEY
            COMMENT 'Surrogate Key for the dimension table',
        profit_center STRING
            COMMENT 'Unique identifier for the profit center',
        profit_center_description STRING 
            COMMENT 'Descriptions are by default English, but if there is no English description then then the French description is taken, and if that is missing as well then the Portuguese description is used.',
        controlling_area STRING 
            COMMENT 'The controlling area',
        segment STRING 
            COMMENT 'Profit Center Segmentr represents the highest level of profit center group (hierarchy) SEG_TOTAL. It is modelled here as a flat attribute here on the Profit Center dimension. Segment contains the abbreviated value, such as SEG_COMM, SEG_LUB and SEG_RETAIL. The descriptions are in segment_description.',
        segment_description STRING 
            COMMENT 'Profit Center segment is a representation of the profit center group SEG_TOTAL in SAP S4HANA. It also reflects a specific level of the standard profit center hierarchy, which is modelled as a flat attribute here on the Profit Center dimension. Example values are Total Commercial, Total Lubes and Total Retail.',
        line_of_business STRING
            COMMENT 'Primary line of business linked to the profit center',
        line_of_business_description STRING
            COMMENT 'Descriptions are by default English, but if there is no English description then then the French description is taken, and if that is missing as well then the Portuguese description is used.',
        line_of_business_1 STRING
            COMMENT 'Line of business 1',
        line_of_business_1_description STRING
            COMMENT 'Line of business 1 Description',
        volume_flag_ind BOOLEAN 
            COMMENT 'The volume flag indicates if the profit center uses volume-based measures for reporting. The value is derived from the segment using the rule: If segment contains SEG_RETAIL, SEG_COMM, SEG_LUB or SEG_SUPPLY then Y else N.',
        sales_organization STRING 
            COMMENT 'Geographical based division, responsible for sales (SD) related processes. In Vivo, this is directly linked to the Profit Center using SAP S4HANA table ZRTR_PRCTR_TAB. This table can have multiple records per Profit Center, only the first one is assigned in the profit_centre_dimension, based on a ranking by sales_org, distribution_channel and division',
        sales_organization_description STRING 
            COMMENT 'Sales Org Desc',
        distribution_channel STRING 
            COMMENT 'Organisational unit specifying how products reach market. Part of the sales_organisation. In Vivo, this is directly linked to the Profit Center using SAP S4HANA table ZRTR_PRCTR_TAB. This table can have multiple records per Profit Center, only the first one is assigned in the profit_centre_dimension, based on a ranking by sales_org, distribution_channel and division',
        distribution_channel_description STRING 
            COMMENT 'Distribution Channel Desc',
        division STRING 
            COMMENT 'Sub grouping of the sales_organisation. In Vivo, this is directly linked to the Profit Center using SAP S4HANA table ZRTR_PRCTR_TAB. This table can have multiple records per Profit Center, only the first one is assigned in the profit_centre_dimension, based on a ranking by sales_org, distribution_channel and division',
        division_description STRING 
            COMMENT 'Division Desc',

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
    ALTER TABLE {env_vars.gold_catalog}.fin_controlling.dim_profit_center
    SET TAGS ('source_system' = 's4hana', 'source_table' = 'CEPC');          
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE {env_vars.gold_catalog}.fin_controlling.dim_profit_center
    ALTER COLUMN profit_center
    SET TAGS ('source_system' = 's4hana', 'source_table' = 'CEPC', 'source_field' = 'PRCTR');
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE {env_vars.gold_catalog}.fin_controlling.dim_profit_center
    ALTER COLUMN profit_center_description
    SET TAGS ('source_system' = 's4hana', 'source_table' = 'CEPCT', 'source_field' = 'LTEXT');
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE {env_vars.gold_catalog}.fin_controlling.dim_profit_center
    ALTER COLUMN segment
    SET TAGS ('source_system' = 's4hana', 'source_table' = 'SETHEADER', 'source_field' = 'Derived from hierarchy');
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE {env_vars.gold_catalog}.fin_controlling.dim_profit_center
    ALTER COLUMN segment_description
    SET TAGS ('source_system' = 's4hana', 'source_table' = 'SETHEADER', 'source_field' = 'Derived from hierarchy');
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE {env_vars.gold_catalog}.fin_controlling.dim_profit_center
    ALTER COLUMN volume_flag_ind
    SET TAGS ('source_system' = 's4hana', 'source_table' = 'SETHEADER', 'source_field' = 'Derived from hierarchy');
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE {env_vars.gold_catalog}.fin_controlling.dim_profit_center
    ALTER COLUMN profit_center
    SET TAGS ('source_system' = 's4hana', 'source_table' = 'CEPC', 'source_field' = 'PRCTR');
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE {env_vars.gold_catalog}.fin_controlling.dim_profit_center
    ALTER COLUMN sales_organization
    SET TAGS ('source_system' = 's4hana', 'source_table' = 'TVKOT', 'source_field' = 'VKORG');
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE {env_vars.gold_catalog}.fin_controlling.dim_profit_center
    ALTER COLUMN sales_organization_description
    SET TAGS ('source_system' = 's4hana', 'source_table' = 'TVKOT', 'source_field' = 'VTEXT');
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE {env_vars.gold_catalog}.fin_controlling.dim_profit_center
    ALTER COLUMN distribution_channel
    SET TAGS ('source_system' = 's4hana', 'source_table' = 'TVTWT', 'source_field' = 'VTWEG');
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE {env_vars.gold_catalog}.fin_controlling.dim_profit_center
    ALTER COLUMN distribution_channel_description
    SET TAGS ('source_system' = 's4hana', 'source_table' = 'TVTWT', 'source_field' = 'VTEXT');
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE {env_vars.gold_catalog}.fin_controlling.dim_profit_center
    ALTER COLUMN division
    SET TAGS ('source_system' = 's4hana', 'source_table' = 'TSPAT', 'source_field' = 'SPART');
''')

# COMMAND ----------

spark.sql(f'''
    ALTER TABLE {env_vars.gold_catalog}.fin_controlling.dim_profit_center
    ALTER COLUMN division_description
    SET TAGS ('source_system' = 's4hana', 'source_table' = 'TSPAT', 'source_field' = 'VTEXT');
''')