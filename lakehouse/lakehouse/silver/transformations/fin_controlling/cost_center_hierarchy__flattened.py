# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

# MAGIC %run ../../../common/hierarchy_flattener

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.csks.yaml"
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("flattened", True)}'

# COMMAND ----------

setheader = spark.table(f"{env_vars.bronze_catalog}.fivetran_s4p.setheadert")
setnode = spark.table(f"{env_vars.bronze_catalog}.fivetran_s4p.setnode")
setleaf = spark.table(f"{env_vars.bronze_catalog}.fivetran_s4p.setleaf")
setclst = spark.table(f"{env_vars.bronze_catalog}.fivetran_s4p.setclst")
setclst = setclst.filter(col("LANGU") == "E")

# COMMAND ----------

flattened_profit_centers = hierarchy_flattener.sap_s4hana_hierarchy_flattener(setheader, setnode, setleaf, setclst, setclass="0101", hier_id=None)

# COMMAND ----------

flattened_profit_centers.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(dest_tablename)

# COMMAND ----------

logger.log.info(f'Overwrite: {dest_tablename}')