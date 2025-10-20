# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

# create table dim_company_code
spark.sql(f'''
  CREATE OR REPLACE TABLE {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code
  (
    company_code_skey BIGINT NOT NULL
      COMMENT 'DWH generated identifier',
    company_code STRING
      COMMENT 'Company code identifier, usually 4 characters long',
    company_name STRING 
      COMMENT 'Name of company',
    country_key STRING 
      COMMENT 'Two-character country code.  If not maintained in S4HANA this defaults to "NL".  For Company code "ZWH2" it is hard-coded to "ZW".',
    city STRING 
      COMMENT 'Usually capital of country for Company Code. Defaults to "Amsterdam" if not maintained in S4HANA IFNULL(ORT01,"Amsterdam")',
    currency_skey STRING
      COMMENT 'Default currency for company code',
    chart_of_accounts STRING 
      COMMENT 'Company code CoA, for Vivo always OP01',
    credit_control_area STRING
      COMMENT 'Vivos credit control area to which this Company Code belongs',
    display_name STRING
      COMMENT 'Often company code name in capitals but could have a different description. Is maintained by Flat File load.',
    reporting_entity STRING
      COMMENT 'Entity for statutary reporting. Attribute assigned by flat-file load',
    geo_region STRING
      COMMENT 'Geographical region. Attribute assigned by flat-file load.',
    vivo_group STRING
      COMMENT 'Grouping of company code for statutory and MI reporting. For example, "Vivo", "SVL", JV".',
    entity_grouping_level_top STRING
      COMMENT 'Entity Hierarchy - top level. Attribute assigned by flat-file load',
    entity_grouping_level_0 STRING
      COMMENT 'Entity Hierarchy - level 0. Attribute assigned by flat-file load',
    entity_grouping_level_1 STRING
      COMMENT 'Entity Hierarchy - level 1. Attribute assigned by flat-file load',
    operating_unit STRING
      COMMENT 'Referred to as "OU".  The value is derived from the company code region and reporting entity attributes.',
    entity_grouping_level_2_geographical STRING
      COMMENT 'Entity Hierarchy level 2 - Geographical. Attribute assigned by flat-file load',
    entity_grouping_level_3_vp_reporting STRING
      COMMENT 'Entity Hierarchy - level 3 - VP Reporting. Attribute assigned by flat-file load',
    region_alternative_2 STRING
      COMMENT 'CASE 
              WHEN Region = "East & South" OR Region = "West" or Region = "Maghreb & Indian Ocean" or Region = "East" or Region = "South" THEN Region 
              ELSE "Non-Operating" END',
    planning_company_code STRING
      COMMENT 'attribute assigned by flat-file load',
    central_credit_country_grouping STRING
      COMMENT 'attribute assigned by flat-file load',
    reporting_entity_ri STRING
      COMMENT 'attribute assigned by flat-file load',

    -- metadata columns
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
    CONSTRAINT pk_dim_company_code PRIMARY KEY (company_code_skey)
  )
  COMMENT 'Company Codes from S4HANA, with some attributes uploaded from flat file and some company codes which are not in SAP also uploaded from flat file.'
  CLUSTER BY
    AUTO;
''')


# COMMAND ----------

# Tag: Table
spark.sql(f'''
ALTER TABLE {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code
SET TAGS ('source_system' = 's4hana', 'source_table' = 'T001');
''')

# COMMAND ----------

# Tag: company_code
spark.sql(f'''
ALTER TABLE {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code
ALTER COLUMN company_code
SET TAGS ('source_system' = 's4hana', 'source_table' = 'T001', 'source_field' = 'BUKRS');
''')

# COMMAND ----------

# Tag: company_name
spark.sql(f'''
ALTER TABLE {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code
ALTER COLUMN company_name
SET TAGS ('source_system' = 's4hana', 'source_table' = 'T001', 'source_field' = 'BUTXT');
''')

# COMMAND ----------

# Tag: country_key
spark.sql(f'''
ALTER TABLE {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code
ALTER COLUMN country_key
SET TAGS ('source_system' = 's4hana', 'source_table' = 'T001', 'source_field' = 'LAND1');
''')

# COMMAND ----------

# Tag: city
spark.sql(f'''
ALTER TABLE {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code
ALTER COLUMN city
SET TAGS ('source_system' = 's4hana', 'source_table' = 'T001', 'source_field' = 'ORT01');
''')

# COMMAND ----------

# Tag: currency_key
spark.sql(f'''
ALTER TABLE {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code
ALTER COLUMN currency_skey
SET TAGS ('source_system' = 's4hana', 'source_table' = 'T001', 'source_field' = 'WAERS');
''')

# COMMAND ----------

# Tag: chart_of_accounts
spark.sql(f'''
ALTER TABLE {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code
ALTER COLUMN chart_of_accounts
SET TAGS ('source_system' = 's4hana', 'source_table' = 'T001', 'source_field' = 'KTOPL');
''')

# COMMAND ----------

# Tag: credit_control_area
spark.sql(f'''
ALTER TABLE {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code
ALTER COLUMN credit_control_area
SET TAGS ('source_system' = 's4hana', 'source_table' = 'T001', 'source_field' = 'KKBER');
''')

# COMMAND ----------

# Tag: display_name
spark.sql(f'''
ALTER TABLE {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code
ALTER COLUMN display_name
SET TAGS ('source_system' = 'Flat File', 'source_table' = 'Company Code - Manual.csv', 'source_field' = 'Display Name');
''')

# COMMAND ----------

# Tag: reporting_entity
spark.sql(f'''
ALTER TABLE {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code
ALTER COLUMN reporting_entity
SET TAGS ('source_system' = 'Flat File', 'source_table' = 'Company Code - Manual.csv', 'source_field' = 'Reporting Entity');
''')

# COMMAND ----------


# Tag: geo_region
spark.sql(f'''
ALTER TABLE {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code
ALTER COLUMN geo_region
SET TAGS ('source_system' = 'Flat File', 'source_table' = 'Company Code - Manual.csv', 'source_field' = 'Region');
''')

# COMMAND ----------

# Tag: vivo_group
spark.sql(f'''
ALTER TABLE {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code
ALTER COLUMN vivo_group
SET TAGS ('source_system' = 'Flat File', 'source_table' = 'Company Code - Manual.csv', 'source_field' = 'Group');
''')

# COMMAND ----------

# Tag: entity_grouping_level_top
spark.sql(f'''
ALTER TABLE {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code
ALTER COLUMN entity_grouping_level_top
SET TAGS ('source_system' = 'Flat File', 'source_table' = 'Company Code - Manual.csv', 'source_field' = 'Entity Grouping Level Top');
''')

# COMMAND ----------

# Tag: entity_grouping_level_0
spark.sql(f'''
ALTER TABLE {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code
ALTER COLUMN entity_grouping_level_0
SET TAGS ('source_system' = 'Flat File', 'source_table' = 'Company Code - Manual.csv', 'source_field' = 'Entity Grouping Level 0');
''')

# COMMAND ----------

# Tag: entity_grouping_level_1
spark.sql(f'''
ALTER TABLE {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code
ALTER COLUMN entity_grouping_level_1
SET TAGS ('source_system' = 'Flat File', 'source_table' = 'Company Code - Manual.csv', 'source_field' = 'Entity Grouping Level 1');
''')

# COMMAND ----------


# Tag: operating_unit
spark.sql(f'''
ALTER TABLE {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code
ALTER COLUMN operating_unit
SET TAGS ('source_system' = 'Flat File', 'source_table' = 'Company Code - Manual.csv','source_field' = 'Derived from Reporting Entity');
''')

# COMMAND ----------

# Tag: entity_grouping_level_2_geographical
spark.sql(f'''
ALTER TABLE {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code
ALTER COLUMN entity_grouping_level_2_geographical
SET TAGS ('source_system' = 'Flat File', 'source_table' = 'Company Code - Manual.csv', 'source_field' = 'Entity Grouping Level 2 Geographical');
''')

# COMMAND ----------

# Tag: entity_grouping_level_3_vp
spark.sql(f'''
ALTER TABLE {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code
ALTER COLUMN entity_grouping_level_3_vp_reporting
SET TAGS ('source_system' = 'Flat File', 'source_table' = 'Company Code - Manual.csv', 'source_field' = 'Entity Grouping Level 3 VP reporting');
''')

# COMMAND ----------

# Tag: region_alternative_2
spark.sql(f'''
ALTER TABLE {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code
ALTER COLUMN region_alternative_2
SET TAGS ('source_system' = 'Flat File', 'source_table' = 'Company Code - Manual.csv',  'source_field' = 'Derived from Region');
''')

# COMMAND ----------

# Tag: planning_company_code
spark.sql(f'''
ALTER TABLE {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code
ALTER COLUMN planning_company_code
SET TAGS ('source_system' = 'Flat File', 'source_table' = 'Company Code - Manual.csv',  'source_field' = 'Planning Company Code');
''')

# COMMAND ----------

# Tag: central_credit_country_grouping
spark.sql(f'''
ALTER TABLE {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code
ALTER COLUMN central_credit_country_grouping
SET TAGS ('source_system' = 'Flat File', 'source_table' = 'Company Code - Manual.csv',  'source_field' = 'Credit Country Grouping');
''')

# COMMAND ----------

# Tag: reporting_entity_ri
spark.sql(f'''
ALTER TABLE {env_vars.gold_catalog}.ca_cross_application_components.dim_company_code
ALTER COLUMN reporting_entity_ri
SET TAGS ('source_system' = 'Flat File', 'source_table' = 'Company Code - Manual.csv',  'source_field' = 'Reporting Entity RI');
''')