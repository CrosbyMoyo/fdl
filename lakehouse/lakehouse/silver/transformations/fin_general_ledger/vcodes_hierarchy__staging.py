# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

# MAGIC %run ../../../common/hierarchy_flattener

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

vcodes_flat_file = spark.table(f"{env_vars.bronze_catalog}.ftp_vbox.1pc_vcodes")

# COMMAND ----------

vcode_hierarchy_flattened = hierarchy_flattener.vcodes_hierarchy_flattener(vcodes_flat_file)

# COMMAND ----------

vcode_hierarchy_flattened = ( vcode_hierarchy_flattened
    .distinct()
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{env_vars.silver_catalog}.fin_general_ledger_staging.vcodes_flat_hierarchy")
)