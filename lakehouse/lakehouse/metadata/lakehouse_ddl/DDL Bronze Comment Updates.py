# Databricks notebook source
import os
from pyspark.sql.functions import col, lower, concat, lit

schema = "finance"
environment = os.getenv("ENV")
catalog = os.getenv("BRZ_CATALOG")

print(f"Environment: {environment}")
print(f"Catalog: {catalog}")
print(f"Schema: {schema}")

# COMMAND ----------


def load_sap_metadata():
    print("Loading SAP metadata tables...")
    dd02t_df = spark.table(f"{catalog}.{schema}.dd02t")  # Table descriptions
    dd03l_df = spark.table(f"{catalog}.{schema}.dd03l")  # Field definitions
    dd04l_df = spark.table(f"{catalog}.{schema}.dd04l")  # Data elements
    dd04t_df = spark.table(f"{catalog}.{schema}.dd04t")  # Data element descriptions

    # Filter for English language
    print("Filtering for English language...")
    dd02t_filtered = dd02t_df.filter(dd02t_df["DDLANGUAGE"] == "E")
    dd04t_filtered = dd04t_df.filter(dd04t_df["DDLANGUAGE"] == "E")

    # Perform the joins
    print("Performing joins to create mapping DataFrame...")
    mapping_df = (
        dd02t_filtered.alias("t")
        .join(dd03l_df.alias("f"), dd02t_filtered["TABNAME"] == dd03l_df["TABNAME"], "inner")
        .join(dd04l_df.alias("e"), dd03l_df["ROLLNAME"] == dd04l_df["ROLLNAME"], "inner")
        .join(dd04t_filtered.alias("d"), dd04l_df["ROLLNAME"] == dd04t_filtered["ROLLNAME"], "left")
        .select(
            dd02t_filtered["TABNAME"].alias("source_table_name"),
            dd02t_filtered["DDTEXT"].alias("table_description"),
            dd03l_df["FIELDNAME"].alias("source_column_name"),
            dd04l_df["ROLLNAME"].alias("data_element_name"),
            dd04t_filtered["DDTEXT"].alias("column_description"),
        )
    )
    print("Mapping DataFrame created.")
    return mapping_df

def read_etl_metadata():
    print("Reading ETL metadata from CSV...")
    return spark.read.format("csv").option('header', 'true').load(
        f"abfss://vivid@vividstorage{environment}.dfs.core.windows.net/metadata/etl_metadata/etl_metadata.csv"
    )

def filter_existing_tables(catalog, schema, mapping_df, etl_metadata_df):
    print("Filtering mapping DataFrame for existing tables...")
    filtered_mapping_df = mapping_df.join(
        etl_metadata_df,
        lower(mapping_df.source_table_name) == lower(col("source_table")),
        "inner",
    )

    existing_tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").select("database", "tableName")
    existing_tables = [
        f"{row['tableName']}"
        for row in existing_tables_df.collect()
    ]

    filtered_mapping_df = filtered_mapping_df.filter(
        lower(concat(col("source_database"), lit("_"), col("source_table_name"))).isin(existing_tables)
    )
    print("Filtered mapping DataFrame created.")
    return filtered_mapping_df

def update_table_comments(catalog, schema, filtered_mapping_df):
    print("Updating table and column comments...")
    distinct_tables = filtered_mapping_df.select("source_database", "source_table_name").distinct().collect()
    total_tables = len(distinct_tables)
    
    for i, row in enumerate(distinct_tables):
        print(f'Updating table: {row["source_database"]}.{row["source_table_name"]} ({i+1}/{total_tables} tables finished)')
        source_database = row["source_database"].lower()
        table_name = row["source_table_name"].lower()
        
        table_metadata = filtered_mapping_df.filter(
            (lower(filtered_mapping_df.source_database) == source_database) &
            (lower(filtered_mapping_df.source_table_name) == table_name)
        )

        table_description = table_metadata.select("table_description").first()["table_description"]
        spark.sql(f"ALTER TABLE {catalog}.{schema}.{source_database}_{table_name} SET TBLPROPERTIES ('comment' = '{table_description}')")
        
        actual_columns = [field.name for field in spark.table(f"{catalog}.{schema}.{source_database}_{table_name}").schema]
        columns = table_metadata.select("source_column_name", "column_description").collect()
        total_columns = len(columns)
        
        for j, col_row in enumerate(columns):
            column_name = col_row["source_column_name"]
            column_comment = col_row["column_description"]
            if column_comment and column_name in actual_columns:
                column_comment = column_comment.replace("'", "\\'")  # Escape single quotes in column_comment
                spark.sql(f"ALTER TABLE {catalog}.{schema}.{source_database}_{table_name} ALTER COLUMN `{column_name}` COMMENT '{column_comment}'")
                print(f'Updated column: {column_name} in table: {source_database}.{table_name} ({j+1}/{total_columns} columns finished)')
    print("Table and column comments updated.")

# Load SAP metadata into DataFrames
mapping_df = load_sap_metadata()

# Read the metadata mapping table from Unity Catalog
etl_metadata_df = read_etl_metadata()

# Filter mapping_df for distinct values present in the "source_table" column in etl_metadata_df
filtered_mapping_df = filter_existing_tables(catalog, schema, mapping_df, etl_metadata_df)

# Update table and column comments
update_table_comments(catalog, schema, filtered_mapping_df)