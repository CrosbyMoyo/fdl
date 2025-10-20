# Databricks notebook source
# MAGIC %md
# MAGIC # **Data Ingestion Demo 06/12/24**

# COMMAND ----------

# MAGIC %md
# MAGIC ---------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC This demo will cover a use case where bad data (empty columns) come as part of the ingestion process. We will use Customer and GL Account Master data for this. Data pipelining, quality and lineage capabilities are implemented using Delta Live Tables (DLT). 
# MAGIC
# MAGIC At first we will show how we can handle bad data by not propagating it to the normal flow table but moving the bad data to quarantine with rules defined on a self-documented configuration table. 
# MAGIC
# MAGIC On the second part we will show how the process will change the behaviour after we change the metadata that configures it and will then start failing the GL Account pipeline and moving data to quarantine on the Customer Master Data pipeline.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Expectations to **_Drop_** the records that violate them

# COMMAND ----------

# MAGIC %md
# MAGIC ### Current Expectations

# COMMAND ----------

# Create a DataFrame with the new rules (including the new row)
new_rules = spark.createDataFrame(
    [
        ('dev', 'bronze', 'kna1_demo', 'geography_not_null', 'MCOD3 IS NOT NULL', 'drop', 'silver'),
        ('dev', 'bronze', 'ska1_demo', 'ktopl_not_null', 'KTOPL IS NOT NULL', 'drop', 'silver'),
        ('dev', 'bronze', 'knvp_demo', 'vkorg_is_not_null', 'VKORG IS NOT NULL', 'drop', 'silver'),
        ('dev', 'bronze', 'kna1_demo', 'land1_is_not_null', 'LAND1 IS NOT NULL', 'drop', 'silver')
    ],
    ['catalog', 'schema', 'table', 'expectation', 'expectation_sql', 'expectation_action', 'target_schema']
)

# Insert the new rules into the 'rules' table
new_rules.write.mode('overwrite').option("mergeSchema", "true").saveAsTable('etl_framework_mvp.silver.rules_demo')
dq_rules = spark.read.format("delta").table("etl_framework_mvp.silver.rules_demo")
# Collect unique table names from the rules
unique_tables = dq_rules.select("table", "schema").distinct().collect()

# COMMAND ----------

display(dq_rules)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tables being ingested

# COMMAND ----------

display(unique_tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **BRONZE KNA1 / KNVP (Customer) / SKA1 (GL Account) Tables (With Bad Data)**

# COMMAND ----------

import pyspark.sql.functions as F
print("BRONZE KNA1")
display(spark.read.format("delta").table("etl_framework_mvp.bronze.kna1_demo").orderBy(F.col("MCOD3").desc_nulls_first(),F.col("LAND1").desc_nulls_first()))
print("BRONZE SKA1")
display(spark.read.format("delta").table("etl_framework_mvp.bronze.ska1_demo").orderBy(F.col("KTOPL").desc_nulls_first()))
print("BRONZE KNVP")
display(spark.read.format("delta").table("etl_framework_mvp.bronze.knvp_demo"))

# COMMAND ----------

# MAGIC %md
# MAGIC # 1st Ingestion Pipeline Runs

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Tables (With No Bad)

# COMMAND ----------

import pyspark.sql.functions as F
print("SILVER KNA1")
display(spark.read.format("delta").table("etl_framework_mvp.silver.silver_kna1_demo").orderBy(F.col("MCOD3").desc_nulls_first(),F.col("LAND1").desc_nulls_first()))
print("SILVER KNVP")
display(spark.read.format("delta").table("etl_framework_mvp.silver.silver_knvp_demo"))
print("SILVER SKA1")
display(spark.read.format("delta").table("etl_framework_mvp.silver.silver_ska1_demo").orderBy(F.col("KTOPL").desc_nulls_first()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set Expectations to **_Fail_** pipelines ingesting data that violates them

# COMMAND ----------

# MAGIC %md
# MAGIC ### Current Expectations

# COMMAND ----------

import pyspark.sql.functions as F
new_rules.withColumn(
    "expectation_action", 
    F.when(F.col("table") == "ska1_demo", F.lit("fail")).otherwise(F.col("expectation_action"))
).write.mode('overwrite').option("mergeSchema", "true").saveAsTable('etl_framework_mvp.silver.rules_demo')

# COMMAND ----------

display(dq_rules)

# COMMAND ----------

# MAGIC %md
# MAGIC # 2nd Ingestion Pipeline Runs