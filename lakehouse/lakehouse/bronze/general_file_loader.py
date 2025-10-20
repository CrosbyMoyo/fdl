# Databricks notebook source
# MAGIC %run ../common/properties

# COMMAND ----------

# MAGIC %md
# MAGIC This Databricks notebook is responsible for loading and processing any type of file from a JSON source stored in Azure Data Lake Storage (ADLS) using Auto Loader. The notebook processes the data based on the folder where the files are coming from and a transformation function that needs to be defined here. The processed data is then written to a Delta table in the Bronze layer.
# MAGIC
# MAGIC Steps performed in this notebook:
# MAGIC 1. Import necessary libraries and retrieve the environment variable (`ENV`) to determine the environment-specific paths.
# MAGIC 2. Define the input data path and checkpoint directory path based on the environment.
# MAGIC 3. Read the JSON data from the input path using Auto Loader (`cloudFiles`) with schema evolution enabled.
# MAGIC 4. Dynamically construct a filter condition to exclude rows where all fields are either NULL or empty.
# MAGIC 5. Apply the filter to the DataFrame to retain only valid rows.
# MAGIC 6. Generate a unique `{FILE_TYPE}_ID` column by hashing the concatenation of `TIMESTAMP` and `EMAIL` fields.
# MAGIC 7. Convert all column names to lowercase for consistency.
# MAGIC 8. Write the processed data to a Delta table in the Bronze layer, using a checkpoint for fault tolerance and ensuring the write operation is triggered once.
# MAGIC
# MAGIC Key Variables:
# MAGIC - `env`: The environment (e.g., dev, prod) retrieved from the `ENV` environment variable.
# MAGIC - `input_data_path`: The path to the input JSON data in ADLS.
# MAGIC - `checkpoint_dir_path`: The path to the checkpoint directory for storing offsets and schema information.
# MAGIC - `filter_condition`: A dynamically constructed condition to filter out invalid rows.
# MAGIC - `{FILE_TYPE}_ID`: A hashed column used as a unique identifier for each record.
# MAGIC
# MAGIC Output:
# MAGIC - The processed data is written to a Delta table in the Bronze layer, under the `powerbi.{file_type}` namespace.
# MAGIC
# MAGIC Note:
# MAGIC - Ensure that the `../common/properties` notebook is correctly configured and provides the necessary environment variables.
# MAGIC - This notebook uses Delta Lake and Databricks Auto Loader for efficient data ingestion and processing.
# MAGIC - Define the transformation function specific to the file type being processed.

# COMMAND ----------

import os
env = os.getenv('ENV')

# COMMAND ----------

dbutils.widgets.text("folder", "inputs", "Folder") # inputs or commentary for now
folder = dbutils.widgets.get("folder")

# COMMAND ----------

from pyspark.sql.functions import sha2, concat_ws, col, lit, explode, from_json, map_keys, map_values
from pyspark.sql.types import MapType, StringType

def get_input_data_path(env, container, storage_account, folder):
    return f"abfss://{container}@{storage_account}{env}.dfs.core.windows.net/{folder}/"

def get_checkpoint_dir_path(env, container, storage_account, folder):
    return f"abfss://{container}@{storage_account}{env}.dfs.core.windows.net/metadata/offsets/{folder}/"

def read_stream_data(input_path, checkpoint_path, format="json"):
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", format)
        .option("cloudFiles.schemaLocation", checkpoint_path)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(input_path)
    )

storage_account="vividstorage"
container="vivid"

input_data_path = get_input_data_path(env, container, storage_account, folder)
checkpoint_dir_path = get_checkpoint_dir_path(env, container, storage_account, folder)

df = read_stream_data(input_data_path, checkpoint_dir_path)


# COMMAND ----------

def manual_inputs_parsing_transformation(df):
    def get_columns_to_pivot(df, columns_to_keep):
        return [column for column in df.columns if column not in columns_to_keep]

    def create_pivot_expressions(df, columns_to_keep, columns_to_pivot):
        return [
            df.select(
                *[col(c) for c in columns_to_keep],
                lit(column).alias("input_type"),
                col(f"`{column}`").alias("value")
            ).filter((col(f"`{column}`").isNotNull()) & (col(f"`{column}`") != ''))
            for column in columns_to_pivot
        ]

    def union_pivoted_dataframes(pivot_expressions):
        df_pivoted = pivot_expressions[0]
        for expr in pivot_expressions[1:]:
            df_pivoted = df_pivoted.union(expr)
        return df_pivoted

    def explode_json_string(df, json_column, new_columns):
        json_schema = MapType(StringType(), StringType())
        return df.withColumn("json_map", from_json(col(json_column), json_schema)) \
                 .withColumn(new_columns[0], explode(map_keys(col("json_map")))) \
                 .withColumn(new_columns[1], col("json_map")[col(new_columns[0])]) \
                 .drop("json_map")

    # Columns to keep
    columns_to_keep = ["ACTIVE_FLAG", "COUNTRY", "GROUP_CODE", "PAGE", "TIMESTAMP", "USER_EMAIL", "USER_NAME"]

    # Get columns to pivot
    columns_to_pivot = get_columns_to_pivot(df, columns_to_keep)

    # Create pivot expressions
    pivot_expressions = create_pivot_expressions(df, columns_to_keep, columns_to_pivot)

    # Union all pivoted dataframes
    df_pivoted = union_pivoted_dataframes(pivot_expressions)

    # Explode the JSON string in the value column
    df_exploded = explode_json_string(df_pivoted, "value", ["period", "value"])

    # Create INPUT_ID column
    df_hashed = df_exploded.withColumn(
        "INPUT_ID", sha2(concat_ws("", "TIMESTAMP", "USER_EMAIL"), 256)
    )

    # Convert all columns to lower case
    df_lower = df_hashed.select([col(c).alias(c.lower()) for c in df_hashed.columns])

    df_filtered = df_lower.filter(col("period") != 'CATEGORY')

    return df_filtered

def commentary_parsing_transformation(df):
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

    return df_lower
    
def execute_transformations(df, transformations):
    for transformation in transformations:
        df = transformation(df)
    return df

folder_to_transformation_mapping = {"inputs": manual_inputs_parsing_transformation, "commentary": commentary_parsing_transformation}
df_transformed = execute_transformations(df, [folder_to_transformation_mapping[folder]])

(
    df_transformed.writeStream.format("delta")
    .option("checkpointLocation", checkpoint_dir_path)
    .option("mergeSchema", "true")
    .trigger(once=True)
    .toTable(f"{env_vars.bronze_catalog}.powerbi.{folder}")
)