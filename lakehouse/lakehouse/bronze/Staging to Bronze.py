# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_date, date_sub, date_format, lit, concat_ws, col, lower
from datetime import datetime, timedelta
import os


# COMMAND ----------

env = os.getenv('ENV')
BRZ_CATALOG = os.getenv("BRZ_CATALOG")


# COMMAND ----------

# 1) Create widgets to capture the parameters from ADF
dbutils.widgets.text('staging_path', '')
dbutils.widgets.text('source_system', '')
dbutils.widgets.text('source_database', '')
dbutils.widgets.text('source_table', '')
dbutils.widgets.text('load_type', '')
dbutils.widgets.text('delta_key', '')
dbutils.widgets.text('running_day', '')


# COMMAND ----------

# 2) Retrieve widget values
staging_path = dbutils.widgets.get('staging_path')
source_system = dbutils.widgets.get('source_system').lower()
source_database = dbutils.widgets.get('source_database')
source_table = dbutils.widgets.get('source_table').lower()
load_type = dbutils.widgets.get('load_type')
delta_key = dbutils.widgets.get('delta_key')
running_day = dbutils.widgets.get('running_day') #20250101 for Date format delta keys, 20250101235959 for Datetime format delta keys

# COMMAND ----------


write_mode = 'overwrite' if load_type == 'full' else 'append'

# Reads the metadata from the specified CSV file.
def read_metadata() -> DataFrame:
    return spark.read.format('csv').option('header', 'true').load(
        f'abfss://vivid@vividstorage{env}.dfs.core.windows.net/metadata/etl_metadata/etl_metadata.csv'
    )

# Reads the source data from the specified path and source type.
def read_source_data(staging_path: str) -> DataFrame:
        return spark.read.format('parquet').load(
            f'abfss://vivid@vividstorage{env}.dfs.core.windows.net/staging/{staging_path}'
        )

# Writes the DataFrame to a Delta table.
def write_to_delta(df: DataFrame, source_table: str, write_mode: str) -> None:
    df.write.format('delta').option('mergeSchema', 'true').mode(write_mode).saveAsTable(
        f'{BRZ_CATALOG}.{source_system}.{source_table}'
    )

# Deletes the manual upload file from the staging path if it exists.
def delete_manual_upload_file(staging_path: str) -> None:
    if 'file_ingestion/' in staging_path:
        dbutils.fs.rm(f'abfss://vivid@vividstorage{env}.dfs.core.windows.net/staging/{staging_path}', True)

def update_offset(metadata_df: DataFrame, source_table: str, delta_key: str) -> None:
    def is_datetime_format(format_string):
        # Check for time-related placeholders
        time_placeholders = ["HH", "mm", "ss"]
        return all(placeholder in format_string for placeholder in time_placeholders)

    delta_key_format = (metadata_df
                        .filter((lower(metadata_df.source_database) == source_database) 
                             & (lower(metadata_df.source_table) == source_table))
                        .select('delta_key_format')
                        .collect()[0][0])
    
    if is_datetime_format(delta_key_format):
        offset = running_day + '235959'
    else:
        offset = running_day

    timestamp_df = spark.createDataFrame([(offset,)], ['delta_ts'])
    temp_path = f'abfss://vivid@vividstorage{env}.dfs.core.windows.net/metadata/offsets/{source_table}/temp_offset'
    final_path = f'abfss://vivid@vividstorage{env}.dfs.core.windows.net/metadata/offsets/{source_table}/offset.csv'
    
    timestamp_df.coalesce(1).write.mode('overwrite').option('header', 'true').csv(temp_path)
    
    files = dbutils.fs.ls(temp_path)
    for file in files:
        if file.name.endswith('.csv'):
            dbutils.fs.mv(file.path, final_path)
    dbutils.fs.rm(temp_path, True)

    return offset



# COMMAND ----------

print('Reading metadata')
metadata_df = read_metadata()
print('Reading source data')
staging_df = read_source_data(staging_path)

# COMMAND ----------

print('Writing data to Delta table')
write_to_delta(staging_df, source_table, write_mode)

# COMMAND ----------

print('Deleting manual upload file if exists')
delete_manual_upload_file(staging_path)
    
if write_mode == 'append':
    print('Updating offset value')
    offset = update_offset(metadata_df, source_table, delta_key)
    print(f'Offset value written {offset}')

record_count = staging_df.count()

print(f'Wrote {record_count} records to {source_table}')