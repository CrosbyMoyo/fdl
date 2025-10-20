# Databricks notebook source
## ACTUAL DDL SECTION ######################################################################################
import os
target_catalog = os.getenv("BRZ_CATALOG")
schema = os.getenv("FINANCE_SCHEMA")
create_table_queries_path = f"create_table_queries/{target_catalog}_{schema}"

# Directory path (DBFS API path)
ddl_directory = f"file:{os.getcwd()}/{create_table_queries_path}/"

# List all files in the directory
print(f"Listing all .txt files in directory: {ddl_directory}")
files = [
    file.path for file in dbutils.fs.ls(ddl_directory) if file.path.endswith(".txt")
]
print(f"Found files: {files}")

# Loop through the files, read their content, and execute the queries
for file_path in files:
    # Read the content of the SQL file
    print(f"Reading content from file: {file_path}")
    file_content = dbutils.fs.head(file_path, 100000).replace("{target_catalog}", target_catalog)
  # Adjust the byte limit as needed

    print(f"Executing SQL from: {file_path}")
    try:
        # Execute the SQL query
        spark.sql(file_content)
        print(f"Executed successfully: {file_path}")
    except Exception as e:
        # Log any errors during execution
        print(f"Error executing SQL from {file_path}: {e}")

############################################################################################################