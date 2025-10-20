# Databricks notebook source
# MAGIC %run Workspace/Users/ahmet.cihan@vivoenergy.com/ViviD%20ETL/etl-framework-mvp/mvp/utilities

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS etl_framework_mvp.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS etl_framework_mvp.silver;

# COMMAND ----------

files = dbutils.fs.ls('abfss://raw@stukssapreportingdev.dfs.core.windows.net/SAP /')
metadata = []
for file in files:
    table_name = file.name.split("/")[0]
    print(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS etl_framework_mvp.metadata;
# MAGIC CREATE OR REPLACE TABLE etl_framework_mvp.metadata.staging_table(
# MAGIC   source_system STRING,
# MAGIC   source_type STRING, 
# MAGIC   source_table STRING,
# MAGIC   table_alias STRING,
# MAGIC   table_columns STRING,
# MAGIC   load_type STRING, 
# MAGIC   delta_key STRING,
# MAGIC   table_type STRING, 
# MAGIC   primary_keys STRING
# MAGIC );

# COMMAND ----------

sap_key_fields = {
    "ACDOCA": {"primary_keys": "RCLNT,RLDNR,RBUKRS,GJAHR,BELNR,DOCLN",
               "load_type": "incremental"},
    "BUT0ID": {"primary_keys": "CLIENT,PARTNER,TYPE,IDNUMBER",
               "load_type": "full"},
    "CEPC": {"primary_keys": "MANDT,PRCTR,DATBI,KOKRS",
             "load_type": "full"},
    "CEPCT": {"primary_keys": "MANDT,SPRAS,PRCTR,DATBI,KOKRS",
              "load_type": "full"},
    "CSKS": {"primary_keys": "MANDT,KOKRS,KOSTL,DATBI",
             "load_type": "full"},
    "CSKT": {"primary_keys": "MANDT,SPRAS,KOKRS,KOSTL,DATBI",
             "load_type": "full"},
    "FINCS_FSIMAPITM": {"primary_keys": "MANDT,RITCLG,KTOPL,MAPPING_ID,REVISION,RACCT",
                        "load_type": "full"},
    "KNA1": {"primary_keys": "KUNNR",
             "load_type": "full"},
    "KNVP": {"primary_keys": "MANDT,KUNNR,VKORG,VTWEG,SPART,PARVW,PARZA",
             "load_type": "full"},
    "MARA": {"primary_keys": "MATNR",
             "load_type": "full"},
    "S4_FINCS_FSIMAPASSG": {"primary_keys": "MANDT,FSMVS,RITCLG,KTOPL,VALIDTODATE",
                            "load_type": "full"},
    "SKA1": {"primary_keys": "MANDT,KTOPL,SAKNR",
             "load_type": "full"},
    "T001": {"primary_keys": "BUKRS",
             "load_type": "full"},
    "TCURC": {"primary_keys": "MANDT,WAERS",
              "load_type": "full"},
    "TCURF": {"primary_keys": "MANDT,KURST,FCURR,TCURR,GDATU",
              "load_type": "full"},
    "TCURN": {"primary_keys": "MANDT,FCURR,TCURR,GDATU",
              "load_type": "full"},
    "TCURR": {"primary_keys": "MANDT,KURST,FCURR,TCURR,GDATU",
              "load_type": "full"},
    "TCURT": {"primary_keys": "MANDT,SPRAS,WAERS",
              "load_type": "full"},
    "TCURV": {"primary_keys": "MANDT,KURST",
              "load_type": "full"},
    "TCURX": {"primary_keys": "CURRKEY",
              "load_type": "full"},
    "UDMBPSEGMENTS": {"primary_keys": "MANDT,PARTNER,COLL_SEGMENT,VALID_UNTIL",
                      "load_type": "full"},
    "ZRTR_PRCTR_TAB": {"primary_keys": "MANDT,VKORG,VTWEG,SPART",
                       "load_type": "full"},
    "ZRTR_VCODES": {"primary_keys": "MANDT,SAKNR,KOSAR",
                    "load_type": "full"}
}

manual_uploads_key_fields = {
    "1PC_FI_FACCCMAPPING_001": {"primary_keys": "Functional_area",
                                "load_type": "full"},
    "1PC_LOBPROFCTR": {"primary_keys": "LOB",
                       "load_type": "full"},
    "1PC_SGRCONSITEM": {"primary_keys": "Company_code",
                        "load_type": "full"},
    "1pc_Vcodes": {"primary_keys": "Child_Vcode",
                   "load_type": "full"},
    "1pc_compcode": {"primary_keys": "company_code",
                     "load_type": "full"},
    "GL_Account_Excluded": {"primary_keys": "GLACcount",
                            "load_type": "full"},
    "dsp_fi_manual_load_jnls": {"primary_keys": "account,entity,icp,ud1,ud2,ud3,ud4,ud5,ud6,period,fy",
                                "load_type": "full"},
    "dsp_fi_manual_load_plan": {"primary_keys": "account,entity,icp,ud1,ud2,ud3,ud4,ud5,ud6,period,fy",
                                "load_type": "full"}
}

for table_name, value in sap_key_fields.items():
    print(table_name)
    table_keys = value["primary_keys"]
    load_type = value["load_type"]
    spark.sql(f"""INSERT INTO etl_framework_mvp.metadata.staging_table 
                (source_system, source_type, source_table, table_alias, table_columns, load_type, delta_key, table_type, primary_keys) 
                VALUES ('SAP', 'database', \'{table_name}\', 'customer', '*', \'{load_type}\', '_etl_effective_from', 'transactional', \'{table_keys}\')
                """)

for table_name, value in manual_uploads_key_fields.items():
    print(table_name)
    table_keys = value["primary_keys"]
    spark.sql(f"""INSERT INTO etl_framework_mvp.metadata.staging_table 
                (source_system, source_type, source_table, table_alias, table_columns, load_type, delta_key, table_type, primary_keys) 
                VALUES ('Manuel_Upload', 'database', \'{table_name}\', 'customer', '*', \'{load_type}\', '_etl_effective_from', 'transactional', \'{table_keys}\')
                """)

# COMMAND ----------

path = "etl_framework_mvp.metadata.staging_table"
df = spark.read.table(path)
single_partition_df = df.repartition(1)
vol_path_dev = "abfss://metadata@stukssapreportingdev.dfs.core.windows.net/etl_metadata/"
single_partition_df.write.mode("overwrite").format("parquet").save(vol_path_dev)
file_list = dbutils.fs.ls(vol_path_dev)

name = [file.name for file in file_list if file.name.endswith('.parquet')][0]
system_names = [file.name for file in file_list if file.name.startswith('_')]

for system_name in system_names:
    dbutils.fs.rm(vol_path_dev + system_name)
# Copy the file into the new location with controlled permissions using UC
dbutils.fs.mv(vol_path_dev + name, vol_path_dev+"/etl_metadata.parquet")
print(vol_path_dev+ name, vol_path_dev+"/etl_metadata.parquet")

# COMMAND ----------

staging_table_path = "etl_framework_mvp.metadata.staging_table"

if spark.catalog.tableExists(staging_table_path):
    df = spark.table("etl_framework_mvp.metadata.staging_table")
    rows = df.select("source_table").dropDuplicates(["source_table"]).collect()
    count = 0
    for row in rows:
        table_name = row["source_table"]
        table_name_path = f"etl_framework_mvp.bronze.brz_{table_name}"
        if spark.catalog.tableExists(table_name_path):
            count += 1
        else:
            print("Table does not exist", table_name)
    if count == len(rows):
        print("All of the tables exist")
    else:
        create_tables()


# COMMAND ----------

# MAGIC %run Workspace/Users/ahmet.cihan@vivoenergy.com/ViviD%20ETL/etl-framework-mvp/mvp/utilities
# MAGIC

# COMMAND ----------

update_tables_merge()

# COMMAND ----------

