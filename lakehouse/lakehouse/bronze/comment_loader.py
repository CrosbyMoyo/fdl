# Databricks notebook source
# MAGIC %run ../common/properties

# COMMAND ----------

# MAGIC %md
# MAGIC This Databricks notebook is responsible for loading and processing commentary data from a JSON source 
# MAGIC stored in Azure Data Lake Storage (ADLS) and writing the processed data to a Delta table in the Bronze layer.
# MAGIC
# MAGIC Steps performed in this notebook:
# MAGIC 1. Import necessary libraries and retrieve the environment variable (`ENV`) to determine the environment-specific paths.
# MAGIC 2. Define the input data path and checkpoint directory path based on the environment.
# MAGIC 3. Read the JSON data from the input path using Auto Loader (`cloudFiles`) with schema evolution enabled.
# MAGIC 4. Dynamically construct a filter condition to exclude rows where all fields are either NULL or empty.
# MAGIC 5. Apply the filter to the DataFrame to retain only valid rows.
# MAGIC 6. Generate a unique `COMMENT_ID` column by hashing the concatenation of `TIMESTAMP` and `COMMENTER_EMAIL`  fields.
# MAGIC 7. Convert all column names to lowercase for consistency.
# MAGIC 8. Write the processed data to a Delta table in the Bronze layer, using a checkpoint for fault tolerance and ensuring the write operation is triggered once.
# MAGIC
# MAGIC Key Variables:
# MAGIC - `env`: The environment (e.g., dev, prod) retrieved from the `ENV` environment variable.
# MAGIC - `input_data_path`: The path to the input JSON data in ADLS.
# MAGIC - `checkpoint_dir_path`: The path to the checkpoint directory for storing offsets and schema information.
# MAGIC - `filter_condition`: A dynamically constructed condition to filter out invalid rows.
# MAGIC - `COMMENT_ID`: A hashed column used as a unique identifier for each comment.
# MAGIC
# MAGIC Output:
# MAGIC - The processed data is written to a Delta table in the Bronze layer, under the `powerbi.commentary` namespace.
# MAGIC
# MAGIC Note:
# MAGIC - Ensure that the `../common/properties` notebook is correctly configured and provides the necessary environment variables.
# MAGIC - This notebook uses Delta Lake and Databricks Auto Loader for efficient data ingestion and processing.
# MAGIC

# COMMAND ----------

import os
env = os.getenv('ENV')

# COMMAND ----------

from pyspark.sql.functions import sha2, concat_ws, col

input_data_path = f"abfss://vivid@vividstorage{env}.dfs.core.windows.net/commentary/"

# add folder to terraform
checkpoint_dir_path = (
    f"abfss://vivid@vividstorage{env}.dfs.core.windows.net/metadata/offsets/commentary/"
)

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", checkpoint_dir_path)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(input_data_path)
)

# Get the schema fields
schema_fields = df.schema.fields

# Construct the filter condition dynamically
filter_condition = " OR ".join(
    [f"{field.name} IS NOT NULL AND {field.name} != ''" for field in schema_fields]
)

# Apply the filter
df_filtered = df.filter(filter_condition)

# Create COMMENT_ID column
df_hashed = df_filtered.withColumn(
    "COMMENT_ID", sha2(concat_ws("", "TIMESTAMP", "COMMENTER_EMAIL"), 256)
)


# Convert all columns to lower case
df_lower = df_hashed.select([col(c).alias(c.lower()) for c in df_hashed.columns])

(
    df_lower.writeStream.format("delta")
    .option("checkpointLocation", checkpoint_dir_path)
    .trigger(once=True)
    .toTable(f"{env_vars.bronze_catalog}.powerbi.commentary")
)

print(f'Data written to {env_vars.bronze_catalog}.powerbi.commentary')