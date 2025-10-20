# Databricks notebook source
from pyspark.sql.functions import *
source_catalog = f"vivid_dev"
target_catalog = f"vivid_dev_brz"
source_schema = "bronze"
target_schema = "finance"

def migrate_data(source_catalog, target_catalog, source_schema, target_schema):
    tables = (
        spark.sql(f"SHOW TABLES IN {source_catalog}.{source_schema}")
        .filter(col("isTemporary") == False)
        .select("tableName")
        .collect()
    )

    tables_list = [row.tableName for row in tables]

    # Migrate data from source_catalog.layer to target_catalog.layer
    for table in tables_list:
        spark.sql(
                f"""
                INSERT INTO {target_catalog}.{target_schema}.{table} 
                SELECT * FROM {source_catalog}.{source_schema}.{table}
                """
            )
        print(f"Data migration for table {table} completed.")

# Example usage
migrate_data(source_catalog, target_catalog, source_schema, target_schema)