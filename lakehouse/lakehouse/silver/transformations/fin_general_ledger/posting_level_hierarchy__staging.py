# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

# MAGIC %run ../../../common/hierarchy_flattener

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

posting_level_hierarchy_flattened = hierarchy_flattener.hrrp_flattener('CS21')

# COMMAND ----------

posting_level_hierarchy_flattened = ( posting_level_hierarchy_flattened
    .distinct()
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{env_vars.silver_catalog}.fin_general_ledger_staging.posting_level_flat_hierarchy")
)