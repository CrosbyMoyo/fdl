# Databricks notebook source
# MAGIC %md
# MAGIC # Schema Evolution Test Case with KNA1
# MAGIC
# MAGIC Create another copy file for KNA1 and test the following scenarios:
# MAGIC
# MAGIC 1. Drop a column
# MAGIC 2. Add a column
# MAGIC 3. Change a column data type (string to int, int to string etc.).
# MAGIC
# MAGIC Check documentation below:
# MAGIC
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/delta/update-schema#merge-evo

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE etl_framework_mvp.bronze.brz_kna1_test1;

# COMMAND ----------

# MAGIC %md
# MAGIC Create a copy of the KNA1 file in the ADLS and drop a column.

# COMMAND ----------

# Path to the Parquet file
parquet_file_path = "abfss://raw@stukssapreportingdev.dfs.core.windows.net/SAPS4/KNA1/Landing/2024/12/03/KNA1(1).parquet"

# Read the Parquet file into a DataFrame
df = spark.read.parquet(parquet_file_path)
dropped_df = df.drop("LAND1")
# Write the updated DataFrame to a new location
dropped_df.write.mode("overwrite").parquet("abfss://raw@stukssapreportingdev.dfs.core.windows.net/SAPS4/KNA1/Landing/2024/12/03/KNA1_updated.parquet")

# COMMAND ----------

updated_parquet_file_path = "abfss://raw@stukssapreportingdev.dfs.core.windows.net/SAPS4/KNA1/Landing/2024/12/03/KNA1_updated.parquet"
df = spark.read.parquet(updated_parquet_file_path)
df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("etl_framework_mvp.bronze.brz_kna1_test1")


# COMMAND ----------

# MAGIC %md
# MAGIC Using the `overwriteSchema` option, the schema is overwritten and the "LAND1" column is removed. 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE etl_framework_mvp.bronze.brz_kna1_test1;

# COMMAND ----------

# MAGIC %md
# MAGIC Edit the KNA1 copy to add a new column called "new_columns" with the value of 100 in every row.

# COMMAND ----------

from pyspark.sql.functions import lit

parquet_file_path = "abfss://raw@stukssapreportingdev.dfs.core.windows.net/SAPS4/KNA1/Landing/2024/12/03/KNA1(1).parquet"

# Read the Parquet file into a DataFrame
df = spark.read.parquet(parquet_file_path)
added_df = df.withColumn("test_column", lit(100))
# Write the updated DataFrame to a new location
added_df.write.mode("overwrite").parquet("abfss://raw@stukssapreportingdev.dfs.core.windows.net/SAPS4/KNA1/Landing/2024/12/03/KNA1_updated.parquet")

# COMMAND ----------

updated_parquet_file_path = "abfss://raw@stukssapreportingdev.dfs.core.windows.net/SAPS4/KNA1/Landing/2024/12/03/KNA1_updated.parquet"
df = spark.read.parquet(updated_parquet_file_path)
df.write.option("mergeSchema", "true").mode("append").saveAsTable("etl_framework_mvp.bronze.brz_kna1_test1")

# COMMAND ----------

# MAGIC %md
# MAGIC This time `mergeSchema` is used to add the column that is added to the source file. From the results, 'new_columns' is added as expected and it has a data_type int.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE etl_framework_mvp.bronze.brz_kna1_test1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY etl_framework_mvp.bronze.brz_kna1_test1;

# COMMAND ----------

# MAGIC %md
# MAGIC Change the added column's type "new_columns" from string to int.

# COMMAND ----------

from pyspark.sql.functions import col

parquet_file_path = "abfss://raw@stukssapreportingdev.dfs.core.windows.net/SAPS4/KNA1/Landing/2024/12/03/KNA1_updated.parquet"

# Read the Parquet file into a DataFrame
df = spark.read.parquet(parquet_file_path)
changed_df = df.withColumn("new_columns", col("new_columns").cast("string"))
# Write the updated DataFrame to a new location
changed_df.write.mode("overwrite").parquet("abfss://raw@stukssapreportingdev.dfs.core.windows.net/SAPS4/KNA1/Landing/2024/12/03/KNA1_updated.parquet")

# COMMAND ----------

updated_parquet_file_path = "abfss://raw@stukssapreportingdev.dfs.core.windows.net/SAPS4/KNA1/Landing/2024/12/03/KNA1_updated.parquet"
df = spark.read.parquet(updated_parquet_file_path)
df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("etl_framework_mvp.bronze.brz_kna1_test1")

# COMMAND ----------

# MAGIC %md
# MAGIC It can be seen that `overwriteSchema` is again needed for schema evolution when a data type changes for one of the columns. 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE etl_framework_mvp.bronze.brz_kna1_test1;