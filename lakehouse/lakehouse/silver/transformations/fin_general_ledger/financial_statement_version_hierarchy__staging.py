# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

# MAGIC %run ../../../common/hierarchy_flattener

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

dbutils.widgets.text("hierarchy_ids", "VIVO, VEZM", "Enter hierarchy ids separeted by comma")

hierarchy_ids = dbutils.widgets.get("hierarchy_ids")
version_list = [value.strip() for value in hierarchy_ids.split(",") if value.strip()]

# COMMAND ----------

fsiit = spark.table(f"{env_vars.silver_catalog}.fin_general_ledger.financial_statement_items_in_structure")
fsti = spark.table(f"{env_vars.silver_catalog}.fin_general_ledger.financial_statement_text_for_items")
fsaigl = spark.table(f"{env_vars.silver_catalog}.fin_general_ledger.financial_statement_assignment_item_gl")
fsvd = spark.table(f"{env_vars.silver_catalog}.fin_general_ledger.financial_statement_version_description")
gl_account = spark.table(f"{env_vars.silver_catalog}.fin_general_ledger.gl_account_master")

# COMMAND ----------

fsv_hierarchy = hierarchy_flattener.financial_statement_version_hierarchy_flattener(fsiit, fsti, fsaigl, fsvd, gl_account, version_list)

# COMMAND ----------

fsv_hierarchy = ( fsv_hierarchy
    .distinct()
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{env_vars.silver_catalog}.fin_general_ledger_staging.financial_statement_version_flat_hierarchy")
)

# COMMAND ----------

# df_start = bronze_fagl_pc.alias("pc").join(
#     bronze_fagl_pc.select("PARENT", col("NEXTN").alias("EXISTING_NEXTN")),
#       (col("pc.PARENT") == col("pc.PARENT")) & 
#       (col("pc.ID") == col("EXISTING_NEXTN")),
#     how="leftanti"
# ).withColumn(
#     "sort_order", when(col("ID").isNotNull(), 1)
# )

# # Step 2: Iteratively assign increasing sort_order
# windowSpec = Window.partitionBy("PARENT").orderBy("sort_order")
# bronze_fagl_pc_sorted = bronze_fagl_pc.alias("a").join(
#     df_start.alias("b"), 
#     ["PARENT", "ID"], 
#     "left"
# ).withColumn(
#     "sort_order", 
#     row_number().over(windowSpec)
# )

# COMMAND ----------

# df_start = fagl_pc_vivo.alias("pc").join(
#     fagl_pc_vivo.select("PARENT", col("NEXTN").alias("EXISTING_NEXTN")),
    #     (col("pc.PARENT") == col("pc.PARENT")) & 
    #     (col("pc.ID") == col("EXISTING_NEXTN")),
#     how="leftanti"
# ).withColumn(
#     "sort_order",
#     when(col("ID").isNotNull(), 1)
# )

# # Step 2: Iteratively assign increasing sort_order
# windowSpec = Window.partitionBy("PARENT").orderBy("sort_order")

# fagl_pc_vivo_sorted = fagl_pc_vivo.alias("a").join(
#     df_start.alias("b"),
#     on=["PARENT", "ID"],
#     how="left"
# ).withColumn(
#     "sort_order",
#     row_number().over(windowSpec)
# )

# COMMAND ----------

# fagl_pc_vivo_sorted.select(
#     "a.ID",
#     "a.PARENT",
#     "a.ERGSL",
#     "a.NEXTN",
#     "sort_order"
# ).display()