# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC The schemas agreed with Nico
# MAGIC
# MAGIC https://dev.azure.com/VivoEnergy/Azure%20SAP%20Data%20Reporting/_wiki/wikis/Azure-SAP-Data-Reporting.wiki/459/RFC-7-Vivo-aligned-Schema-naming-standard
# MAGIC
# MAGIC
# MAGIC | Catalog Name | Catalog Description | Schema Name  | Schema Description | Tags |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | vivid_dev_brz | VIVID Dev Bronze | sap_s4hana | SAP S/4Hana |  |
# MAGIC | vivid_dev_brz | VIVID Dev Bronze | lt | Local Table |  |
# MAGIC | vivid_dev_brz | VIVID Dev Bronze | pc | CSV or Excel File |  |
# MAGIC | vivid_dev_brz | VIVID Dev Bronze | sap_sf | Success Factor |  |
# MAGIC | vivid_dev_brz | VIVID Dev Bronze | oracle_hfm | HFM |  |
# MAGIC | vivid_dev_brz | VIVID Dev Bronze | sap_sac | SAP Analytics Cloud |  |
# MAGIC | vivid_dev_brz | VIVID Dev Bronze | sap_ibp | SAP Integrated Business Planning |  |
# MAGIC | vivid_dev_brz | VIVID Dev Bronze | sap_car | SAP CAR |  |
# MAGIC | vivid_dev_brz | VIVID Dev Bronze | azure_asr | Azure ASR |  |
# MAGIC | vivid_dev_brz | VIVID Dev Bronze | azure_atg | Azure ATG |  |
# MAGIC | vivid_dev_brz | VIVID Dev Bronze | op_biolap | Engen BI OLAP |  |
# MAGIC | vivid_dev_brz | VIVID Dev Bronze | op_bistage | Engen BI Stage |  |
# MAGIC | vivid_dev_brz | VIVID Dev Bronze | amazon_sama | SAMA Redshift Vehicles |  |

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Fivetran schemas

# COMMAND ----------

# MAGIC %run ../../common/properties

# COMMAND ----------

# schemas common to all envs
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.bronze_catalog}.capex_sql_server')
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.bronze_catalog}.ftp_vbox')
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.bronze_catalog}.sap_datasphere')
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.bronze_catalog}.sap_s4hana')
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.bronze_catalog}.sap_sac')

spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.bronze_catalog}.powerbi')

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.bronze_catalog}.fivetran_s4p')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.bronze_catalog}.fivetran_s4p__internal')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.bronze_catalog}.fivetran_s4q')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.bronze_catalog}.fivetran_s4q__internal')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.bronze_catalog}.fivetran_s4r')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.bronze_catalog}.fivetran_s4r__internal')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.bronze_catalog}.fivetran_s4x')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.bronze_catalog}.fivetran_s4x__internal')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.bronze_catalog}.fivetran_s4d')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.bronze_catalog}.fivetran_s4d__internal')

    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.bronze_catalog}.sap_s4hana__staging')
elif env_vars.env == 'int':
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.bronze_catalog}.fivetran_s4p')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.bronze_catalog}.fivetran_s4p__internal')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.bronze_catalog}.fivetran_s4q')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.bronze_catalog}.fivetran_s4q__internal')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.bronze_catalog}.fivetran_s4r')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.bronze_catalog}.fivetran_s4r__internal')

    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.bronze_catalog}.sap_s4hana__staging')
else:
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.bronze_catalog}.fivetran_s4p')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.bronze_catalog}.fivetran_s4p__internal')

    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {env_vars.bronze_catalog}.sap_s4hana__staging')