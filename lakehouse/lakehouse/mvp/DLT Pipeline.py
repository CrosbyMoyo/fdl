# Databricks notebook source
schema_name = "vivid_dev.bronze"

tables_df = spark.sql(f"SHOW TABLES IN {schema_name}")

# COMMAND ----------

etl_metadata_file_path = 'abfss://vivid@stukssapreportingdev.dfs.core.windows.net/metadata/etl_metadata'

metadata_df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(etl_metadata_file_path)
metadata_df.display()

# COMMAND ----------

def create_bronze_tables_with_quarantine(table_name, rule):
    @dlt.table(
        name=f"brz_{table_name}"
    )
    @dlt.expect_all(rule)
    def get_table():
        return (
            spark.readStream
                    .table(f"{schema_name}.{table_name}")
                    .withColumn("is_quarantined", ~expr(" AND ".join(rule.values())))
        )
    
    @dlt.table(name=f"quarantine_{table_name}",comment=f"This is a quarantine table quarantine_{table_name}")
    def quarantine():
        return(
            dlt.read_stream(f"brz_{table_name}").filter(col("is_quarantined") == True)
            )

def create_silver_tables(table_name):
    @dlt.table(
        name = f"slv_{table_name}"
    )
    def get_table():
        return dlt.read_stream(f"brz_{table_name}").filter(col("is_quarantined") == False)


# COMMAND ----------

import dlt
from pyspark.sql.functions import col, expr

for row in tables_df.collect():
    table_name = row['tableName']
    primary_keys = metadata_df.filter(col("table_alias") == table_name).select("primary_keys").collect()[0]["primary_keys"]
    
    rule = {f'valid_pk_{table_name}': f'{primary_keys} IS NOT NULL'}

    create_bronze_tables_with_quarantine(table_name, rule)
    create_silver_tables(table_name)
