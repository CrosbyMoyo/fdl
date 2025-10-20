# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp, lit, md5, concat


# COMMAND ----------

schema = ["source_system", "source_type", "source_table", "table_alias", "table_columns", "load_type", "delta_key", "table_type", "primary_keys", "expectation", "expectation_sql", "expectation_action"]

data = [("SAPS4", "database", "KNA1", "customer", "*", "full", "last_changed_time", "transactional", "KUNNR", "Valid Customer Number", "KUNNR IS NOT NULL", "FAIL"), ("SAPS4", "database", "KNA1", "customer", "*", "full", "last_changed_time", "transactional", "KUNNR", "Non-Null Value", "STRAS IS NOT NULL", "DROP"), ("SAPS4", "database", "SKA1", "customer", "*", "full", "last_changed_time", "transactional", "MANDT,KTOPL,SAKNR", "InValid MANDT", "MANDT = '200'", "FAIL"), ("SAPS4", "database", "KNVP", "customer", "*", "full", "last_changed_time", "transactional", "MANDT,KUNNR,VKORG,VTWEG,SPART,PARVW,PARZA", "Valid Customer Number", "PARZA IS NOT NULL", "DROP"), ("SAPS4", "database", "ACDOCA", "customer", "*", "full", "last_changed_time", "transactional", "RCLNT,BELNR,DOCLN,RLDNR,RBUKRS,GJAHR", "Valid Customer Number", "RCLNT IS NOT NULL", "FAIL"), ("SAPS4", "database", "ZRTR_PRCTR_TAB", "customer", "*", "full", "last_changed_time", "transactional", "MANDT,VKORG,VTWEG,SPART", "Valid Customer Number", "VKORG IS NOT NULL", "FAIL")]


# COMMAND ----------

df = spark.createDataFrame(data, schema)

spark.sql("CREATE SCHEMA IF NOT EXISTS etl_framework_mvp.staging")

df.write.option("mergeSchema", "true").mode("overwrite").saveAsTable("etl_framework_mvp.staging.staging_table_test")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM etl_framework_mvp.staging.staging_table_test;

# COMMAND ----------

# metadata table to help to copy files into landing directory
df = spark.table("etl_framework_mvp.staging.staging_table_test")
rows = df.collect()

# COMMAND ----------

from pyspark.sql.functions import col
def get_rules(table_name, expectation_action):
    """
    Returns the expectations as a dictionary from the metadata table.

    Parameters:
        table_name (str): The table name for expectations.
        expectation_action (str): The expectation action for filtering expectations (fail, drop, etc)
    
    Returns:
        dict: Return a dictionary to use in the expect_or_reject function.

    """
    # read the metadata table
    df = spark.table("etl_framework_mvp.staging.staging_table_test")
    # create an empty dictionary for gathering the expectations
    rules = {}
    # iterate through the rows with the given table name and add the expectation to the dictionary
    for row in df.filter((col("source_table") == table_name) & (col("expectation_action") == expectation_action)).collect():
        rules[row['expectation']] = row['expectation_sql']
    return rules


# COMMAND ----------

import datetime

raw_load_date = datetime.datetime.now()
load_date = raw_load_date.strftime('%Y%m%d%H%M%S')
load_date_year = raw_load_date.strftime('%Y')
load_date_month = raw_load_date.strftime('%m')
load_date_day = raw_load_date.strftime('%d')

# COMMAND ----------

# MAGIC %md
# MAGIC The code below iterates through the metadata table to create the landing files with the required naming format. Three files are used for this implementation, KNA1, SKA1, and KNVP.

# COMMAND ----------

import datetime

file_list = ['KNA1','SKA1', 'KNVP']

# metadata table to help to copy files into landing directory
df = spark.table("etl_framework_mvp.staging.staging_table_test")
rows = df.collect()

# get the current date information for creation of landing directory and the file name
raw_load_date = datetime.datetime.now()
load_date = raw_load_date.strftime('%Y%m%d%H%M%S')
load_date_year = raw_load_date.strftime('%Y')
load_date_month = raw_load_date.strftime('%m')
load_date_day = raw_load_date.strftime('%d')

# iterate through the file lists that are already extracted from SAP
for file in file_list:
    extracted_file_path = f'abfss://raw@stukssapreportingdev.dfs.core.windows.net/SAP / {file} /2024/11/22' # date can be changed to be dynamic but since it is only landing and temp not necessary and can be static
    # iterate through the metadata table to create landing directories and rename the copied file
    for row in rows:
        source_system = row.source_system
        source_table = row.source_table

        landing_file_path = f'abfss://raw@stukssapreportingdev.dfs.core.windows.net/{source_system}/{source_table}/Landing/{load_date_year}/{load_date_month}/{load_date_day}/'

        renamed_file_path = f'{landing_file_path}/{source_system}_{source_table}_{load_date}.parquet'

        # copy the file to the landing directory
        dbutils.fs.cp(extracted_file_path, landing_file_path, recurse=True)

        # rename the copied file in the lakehouse arhcitecture format
        dbutils.fs.mv(f"{landing_file_path}{file}.parquet", renamed_file_path)


        


# COMMAND ----------


def path_exists(path):
    """
    Checks if the path does exist in the blob storage.

    Parameters:
        path (str): The path to check.
    
    Returns:
        bool: True if the path exists, False otherwise.
    """
    try:
        # Try to list the directory contents
        dbutils.fs.ls(path)
        return True
    except Exception:
        # If an error occurs, assume the path doesn't exist
        return False

# COMMAND ----------

spark.sql("USE CATALOG etl_framework_mvp")
spark.sql("USE bronze")

# iterate through the metadata table rows
for row in rows:
    source_system = row.source_system
    source_table = row.source_table
    load_type = row.load_type
    table_columns = row.table_columns

    landing_file_path = f'abfss://raw@stukssapreportingdev.dfs.core.windows.net/{source_system}/{source_table}/Landing/{load_date_year}/{load_date_month}/{load_date_day}'

    # depending on the load type apply a logic for loading the data
    if load_type == 'full':
    # successful reads writes to bronze tables and creates the loaded path
        if path_exists(landing_file_path):
            try:
                spark.read\
                .format("parquet")\
                .load(landing_file_path)\
                .write.format("delta")\
                .saveAsTable(f'etl_framework_mvp.bronze.brz_{source_table}_test1')

                loaded_file_path = f'abfss://raw@stukssapreportingdev.dfs.core.windows.net/{source_system}/{source_table}/Loaded/{load_date_year}/{load_date_month}/{load_date_day}'

                # move the file from landing to loaded
            except Exception as e:
                # catch any errors during the read/write process
                error_message = f"Error: {str(e)} at {raw_load_date}"
                dbutils.fs.put(f"abfss://raw@stukssapreportingdev.dfs.core.windows.net/{source_system}/{source_table}/Error/{load_date_year}/{load_date_month}/{load_date_day}/error_log.txt", error_message, overwrite=True)

        # if the file does not exist in landed directory
        else:
            error_message = f"Landing file not found at {landing_file_path} at {raw_load_date} \nCheck if the data has already been loaded, or the data may not have been landed in the landing directory."
            dbutils.fs.put(f"abfss://raw@stukssapreportingdev.dfs.core.windows.net/{source_system}/{source_table}/Error/{load_date_year}/{load_date_month}/{load_date_day}/error_log.txt", error_message, overwrite=True)

            # idea: databricks internal alert system can be added to error handling

    else:
        # incremental load logic can be applied here
        pass


# COMMAND ----------

# MAGIC %md
# MAGIC The table brz_kna1_test1 will change with the code below. This is to test the handling bad records strategy.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO your_delta_table
# MAGIC SELECT * FROM your_delta_table WHERE KUNNR = '0001333892';

# COMMAND ----------

from pyspark.sql.functions import col
from delta.tables import DeltaTable
# Create a DeltaTable object
delta_table = DeltaTable.forName(spark, "etl_framework_mvp.bronze.brz_knvp_test1")

# Get the latest version from the history
latest_version = delta_table.history(1).select("version").collect()[0]["version"]

changes_df = spark.read.format("delta").option("readChangeData", "true").option("startingVersion", latest_version).table("etl_framework_mvp.bronze.brz_knvp_test1")

changes_df.display()

# COMMAND ----------

file_path = f'abfss://raw@stukssapreportingdev.dfs.core.windows.net/SAP / ACDOCA /2024/11/29'
source_df = (spark.read
    .format("parquet")
    .load(file_path)
)

source_df.display()

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, to_timestamp, lit, md5, concat, sha2, coalesce, concat_ws

def create_tables():
    """
    Creates the bronze tables for the tables listed in the metadata table.

    Returns:
        None

    """
    # read the metadata table as a dataframe and collect the distinct table names
    df = spark.table("etl_framework_mvp.staging.staging_table_test")
    rows = df.select("source_table", "primary_keys").dropDuplicates(["source_table"]).collect()

    # iterate through the metadata table rows and extract the table names and the primary keys
    for row in rows:
        table_name = row["source_table"]
        primary_keys = row["primary_keys"].split(",")

    # drop the table to recreate if it already exists temproary approach for now and should be removed for future development
        spark.sql(f"""
                  DROP TABLE IF EXISTS {table_name.lower()}
                  """)
        
    # the path in the ADLS where the data sits - with tools like fivetran coming in this wont be necessary but can be used for other sources.
        file_path = f'abfss://raw@stukssapreportingdev.dfs.core.windows.net/SAP / {table_name} /2024/11/24'
        source_df = (spark.read
            .format("parquet")
            .load(file_path)
        )
        
        
        hashed_pk = md5(concat(*[col(pk) for pk in primary_keys]))
        data_columns = [col for col in source_df.columns if col not in primary_keys and col not in ["Azure_Ingested_Date"]]
        hashed_data_source = sha2(
            concat_ws("|", *[coalesce(col(c).cast("string"), lit("NULL")) for c in data_columns]),
            256
        ) 
    # add the metadata fields
        etl_load_df = (
            source_df
            .withColumn(f"data_hash", hashed_data_source)
            .withColumn(f"pk_{table_name.lower()}", hashed_pk)  # Generate a unique PK using the given primary keys in the metadata table
            .withColumn("_etl_effective_from", current_timestamp())  # Effective from time
            .withColumn("_etl_effective_to", to_timestamp(lit("9999-12-31")))  # Far future date for effective to
            .withColumn("_etl_active_flag", lit(True))  # Active flag set to True
        )
    # write the data to the bronze tables
        etl_load_df.write\
            .option("overwriteSchema", "true")\
            .mode("overwrite")\
            .saveAsTable(f'etl_framework_mvp.bronze.brz_{table_name.lower()}_test1')
    # enable cdf deletion vectors and row tracking
        spark.sql(f"""
                    ALTER TABLE etl_framework_mvp.bronze.brz_{table_name.lower()}_test1
                    SET TBLPROPERTIES (
                        delta.enableChangeDataFeed = true,
                        delta.enableDeletionVectors = true,
                        delta.enableRowTracking = true
                    )
                  """)
    # enabling liquid clustering, this ideally should not be the pk but for testing purposes pk has been used.
        # spark.sql(f"""
        #     ALTER TABLE etl_framework_mvp.bronze.brz_{table_name.lower()}_test1
        #     CLUSTER BY (pk_{table_name.lower()})
        #     """)
create_tables()

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import md5, concat, col, current_timestamp, to_timestamp, lit, sha2, coalesce, expr, concat_ws, rand

def update_tables_merge():
    """
    Updates the the tables that are in the staging metadata table by taking a new file from the ADLS.

    Returns:
        None
    """
    
    # read the staging metadata table and create rows according to distinct table names
    df = spark.table("etl_framework_mvp.staging.staging_table_test")
    rows = df.select("source_table", "primary_keys").dropDuplicates(["source_table"]).collect()

    # iterate through the staging table rows and extract the table names and the primary keys
    for row in rows:
        table_name = row["source_table"]
        print(table_name)
        primary_keys = row["primary_keys"].split(",")


        # the new file path from ADLS -> this can be made more dynamic according to the extraction logic
        file_path = f'abfss://raw@stukssapreportingdev.dfs.core.windows.net/SAP / {table_name} /2024/11/27'

        # read the new file from ADLS and create a source dataframe
        source_df = spark.read.format("parquet").load(file_path)

        # generate Primary Key and data hash for source data (do not include 'Azure_Ingested_Date' to data hashing)
        hashed_pk = md5(concat(*[col(pk) for pk in primary_keys]))
        data_columns = [col_name for col_name in source_df.columns if col_name not in primary_keys and col_name not in ["Azure_Ingested_Date"]]
        hashed_data_source = sha2(
            concat_ws("|", *[coalesce(col(c).cast("string"), lit("NULL")) for c in data_columns]),
            256
        )        
        # add the 'pk_(table name)' and 'data_hash' columns to the source dataframe
        source_with_pk = source_df.withColumn(f"pk_{table_name.lower()}", hashed_pk) \
                                  .withColumn("data_hash", hashed_data_source) \
                                  .withColumn("_etl_effective_from", current_timestamp()) \
                                  .withColumn("_etl_effective_to", to_timestamp(lit("9999-12-31"))) \
                                  .withColumn("_etl_active_flag", lit(True))

        # define the target table that is already in the bronze layer
        target_table_name = f'etl_framework_mvp.bronze.brz_{table_name.lower()}_test1'
        delta_table = DeltaTable.forName(spark, target_table_name) 

        # aliases for merge
        source_alias = source_with_pk.alias("source")
        delta_table_alias = delta_table.alias("target")

        merge_condition = f"source.pk_{table_name.lower()} = target.pk_{table_name.lower()}"

        if table_name == 'ACDOCA':
            delta_table.alias("target").merge(
            source=source_alias,
            condition=merge_condition
                ).whenMatchedUpdate(
                    condition="source.data_hash != target.data_hash AND target._etl_active_flag = true",
                    set = {**{col_name: col(f"source.{col_name}") for col_name in source_with_pk.columns}}
                ) \
                .whenNotMatchedInsert( # insert new records
                    values={
                        **{col_name: col(f"source.{col_name}") for col_name in source_with_pk.columns}
                    }
                ).execute()
        else:
            # merge the target table with the source dataframe
            delta_table.alias("target").merge(
                source=source_alias,
                condition=merge_condition
            ).whenMatchedUpdate(
                condition="source.data_hash != target.data_hash AND target._etl_active_flag = true",
                set = {**{col_name: col(f"source.{col_name}") for col_name in source_with_pk.columns}}
            ) \
            .whenNotMatchedInsert( # insert new records
                values={
                    **{col_name: col(f"source.{col_name}") for col_name in source_with_pk.columns}
                }
            ) \
            .whenNotMatchedBySourceUpdate(
                condition="target._etl_active_flag = true",
                set = {"_etl_active_flag": "false",
                        "_etl_effective_to": current_timestamp()}
            ).execute()

                 # get the latest version for tracking changes with CDF
        latest_version = delta_table.history(1).select("version").collect()[0]["version"]

        # CDF is used for tracking changes in the target table after the merge is executed

        # create a changes dataframe which has the CDF metadata
        changes_df = spark.read.format("delta").option("readChangeData", "true").option("startingVersion", latest_version).table(f"etl_framework_mvp.bronze.brz_{table_name.lower()}_test1")
        changes_df.display()

        # # get the preimage updates from the changes dataframe
        preimage_updates = changes_df.filter((col("_change_type") == "update_preimage"))
        # check if there are any updates if there is not this logic will not be applied
        if not preimage_updates.isEmpty():
            # change the metadata to active flag to false and effective to to current timestamp for updated records and append them to the target table
            preimage_updates = preimage_updates.withColumn("_etl_effective_to", current_timestamp()) \
                                               .withColumn("_etl_active_flag", lit(False)) \
                                               .drop("_change_type", "_commit_version", "_commit_timestamp")

            preimage_updates.write.option("mergeSchema", "true").format("delta").mode("append").saveAsTable(target_table_name)

update_tables_merge()

# COMMAND ----------

def to_silver():
    df = spark.table("etl_framework_mvp.staging.staging_table_test")
    rows = df.select("source_table", "primary_keys").dropDuplicates(["source_table"]).collect()

    for row in rows:
        table_name = row["source_table"]
        primary_keys = row["primary_keys"].split(",")

        silver_df = spark.table(f"etl_framework_mvp.bronze.brz_{table_name.lower()}_test1")

        transformed_silver_df = silver_df.filter(col("_etl_active_flag") == True)

        transformed_silver_df.write.format("delta").mode("append").saveAsTable(f"etl_framework_mvp.silver.slv_{table_name.lower()}_test1")
to_silver()
    

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from delta.tables import DeltaTable
from pyspark.sql.functions import lit, current_timestamp
from datetime import datetime

# Create schema for our tables
schema = StructType([
    StructField("id", StringType(), False),
    StructField("value", StringType(), False),
    StructField("current_record", BooleanType(), False),
    StructField("start_date", StringType(), True),
    StructField("end_date", StringType(), True)
])

# Create target data - initial state
target_data = [
    # Regular records
    ("1", "Apple", True, "2024-01-01", None),
    ("2", "Banana", True, "2024-01-01", None),
    ("3", "Cherry", True, "2024-01-01", None),
    # Already historical record
    ("4", "Old_Date", False, "2024-01-01", "2024-01-15"),
    ("4", "Date", True, "2024-01-15", None)
]

# Create source data - new state
source_data = [
    # No change record
    ("1", "Apple"),
    # Value update
    ("2", "Better_Banana"),
    # No change record
    ("3", "Cherry"),
    # Update to previously updated record
    ("4", "Fresh_Date"),
    # Completely new record
    ("5", "Elderberry")
]

# Create DataFrames
target_df = spark.createDataFrame(target_data, schema)
source_df = spark.createDataFrame(source_data, ["id", "value"])

target_df.write.mode("overwrite").saveAsTable("etl_framework_mvp.scd.test_data")
delta_table = DeltaTable.forName(spark, "etl_framework_mvp.scd.test_data")

source_alias = source_df.alias("source")
delta_table_alias = delta_table.alias("target")


# Perform the merge
merge_statement = (
    delta_table.alias("target").merge(
        source = source_alias,
        condition="target.id = source.id AND target.current_record = true"
    )
    .whenMatchedUpdate(
        condition="target.value <> source.value",
        set={
            "current_record": "false",
            "end_date": current_timestamp()
        }
    )
    .whenMatchedInsertAll(
        condition="target.value <> source.value",
        values={
            "id": "source.id",
            "value": "source.value",
            "current_record": "true",
            "start_date": current_timestamp(),
            "end_date": "null"
        }
    )
    .whenNotMatchedInsertAll()
)

# Execute merge
merge_statement.execute()

delta_table.display()

# COMMAND ----------

~exi.merge(
    source_df,
    condition="target.id = source.id AND target.current_record = true"
)
.whenMatchedUpdate(
    condition="target.data_hash != source.data_hash",  # Only update when values differ
    set={
        "current_record": "false",
        "end_date": current_timestamp()
    }
)
.whenMatchedInsertAll(
    condition="target.value <> source.value",  # Insert new version only when values differ
    values={
        "id": "source.id",
        "value": "source.value",
        "current_record": "true"
    }
)
.whenNotMatchedInsertAll()  # For completely new records
).exectue()

# COMMAND ----------

from delta.tables import DeltaTable

# Corrected table name
delta_table = DeltaTable.forName(spark, "etl_framework_mvp.scd.test_data")

# Get the latest version from the history
latest_version = delta_table.history(1).select("version").collect()[0]["version"]

changes_df = spark.read.format("delta").option("readChangeData", "true").option("startingVersion", latest_version).table("etl_framework_mvp.scd.test_data")

display(changes_df)

# COMMAND ----------

delta_table = DeltaTable.forName(spark, f"etl_framework_mvp.scd.test_data")
existing_df = delta_table.toDF()

delta_table = DeltaTable.forName(spark, f"etl_framework_mvp.scd.test_more_data")
source_df = delta_table.toDF()

source_alias = source_df.alias("source")
existing_alias = existing_df.alias("existing")

new_records = source_alias.join(
    existing_alias,
    col(f"source.pk_table") == col(f"existing.pk_table"),
    "left_anti"
)

# Identify changed records
changed_records = (
    source_df.alias("source")
    .join(
        existing_df.alias("existing"), 
        col(f"source.pk_table") == col(f"existing.pk_table"),
        "inner"
    )
    .filter(col("source.data_hash") != col("existing.data_hash"))
    .select("source.*")
)

records_to_write = (
    # New records
    new_records.withColumn("change_type_custom", lit("insert"))
    
    # Changed records
    .union(
        changed_records.withColumn("change_type_custom", lit("update"))
    )
)

# Write with CDF tracking
records_to_write.write \
    .option("mergeSchema", "true") \
    .mode("append") \
    .saveAsTable(f'etl_framework_mvp.scd.test_data')

# COMMAND ----------

new_records.display()

# COMMAND ----------

changed_records.display()

# COMMAND ----------

records_to_write.display()

# COMMAND ----------

merged_df.display()

# COMMAND ----------

def add_records(table_name, column_name):
    """
    Add a record to the specified bronze table for testing purposes.

    Parameters:
        table_name (str): The name of the bronze table to add the record to.
        column_name (str): The name of the column to modify.
    """
    existing_df = spark.table(f"etl_framework_mvp.bronze.brz_{table_name.lower()}_test1")

    # Select a record to update
    record_to_update = existing_df.filter(col("KUNNR") == "0001384876").limit(1)  # Replace with an existing KUNNR value

    # Simulate a change in another column (e.g., "NAME1")
    updated_record = record_to_update.withColumn("NAME1", lit("Test Name"))

    # Add the updated record to the source DataFrame
    updated_source_df = existing_df.union(updated_record)

    updated_source_df.write\
                     .option("mergeSchema", "true")\
                     .mode("append")\
                     .saveAsTable(f'etl_framework_mvp.bronze.brz_{table_name.lower()}_test1')


# COMMAND ----------

from pyspark.sql.functions import lit

def add_bad_null_record(source_table, column_name, rows_to_duplicate=1):
    """
    Adds a bad record to the specified bronze table for testing purposes.

    Parameters:
        source_table (str): The name of the bronze table to add the bad record to.
        rows_to_duplicate (int): The number of rows to duplicate.
        column_name (str): The name of the column to modify.
    
    Returns:
        Modifies the specified bronze table to add a bad record.
    """
    # Load Bronze table
    bronze_df = spark.table(f"etl_framework_mvp.bronze.brz_{source_table.lower()}_test1")

    # Select a row to duplicate
    row_to_duplicate = bronze_df.limit(rows_to_duplicate)

    # Create a bad record by modifying a column
    bad_record = row_to_duplicate.withColumn(f"{column_name}", lit(None))

    # Save the updated dataset back to the Bronze table
    bad_record.write.option("mergeSchema", "true").mode("append").saveAsTable(f"etl_framework_mvp.bronze.brz_{source_table}_test1")

# COMMAND ----------

def drop_bad_null_records(source_table, column_name):
    """
    Removes the bad null record with the given column name from the specified bronze table.

    Parameters:
        source_table (str): The name of the bronze table to remove the bad record from.
        column_name (str): The name of the column to remove the bad record from.

    Returns:
        Overwrites the specified bronze table to remove the bad record.
    """
    spark.sql(f"""
              DELETE FROM etl_framework_mvp.bronze.brz_{source_table}_test1 WHERE {column_name} IS NULL;
              """)

    # bronze_df = spark.table(f"etl_framework_mvp.bronze.brz_{source_table}_test1")
    # dropped_df = bronze_df.dropna(subset=[column_name])

    # dropped_df.write.option("mergeSchema", "true").mode("overwrite").saveAsTable(f"etl_framework_mvp.scd.brz_{source_table}_dlt")

# COMMAND ----------

def update_tables_duplicate():
    df = spark.table("etl_framework_mvp.staging.staging_table_test")
    rows = df.select("source_table", "primary_keys").dropDuplicates(["source_table"]).collect()
    for row in rows:
        table_name = row["source_table"]
        primary_keys = row["primary_keys"].split(",")

        hashed_pk = md5(concat(*[col(pk) for pk in primary_keys]))

        file_path = f'abfss://raw@stukssapreportingdev.dfs.core.windows.net/SAP / {table_name} /2024/12/01'

        source_df = spark.read\
            .format("parquet")\
            .load(file_path)

        source_df.write.option("mergeSchema", "true").mode("append").saveAsTable(f"etl_framework_mvp.bronze.brz_{table_name}_test1")


# COMMAND ----------

update_tables_duplicate()

# COMMAND ----------

add_bad_null_record("kna1", "STRAS", 100)
add_bad_null_record("knvp", "PARZA", 500)

# failing record
# add_bad_null_record("kna1", "KUNNR", 1)

# COMMAND ----------

drop_bad_null_records("knvp", "PARZA")

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, md5, concat

add_records("kna1", "KUNNR")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE etl_framework_mvp.scd.source_table(
# MAGIC     id STRING,
# MAGIC     name STRING,
# MAGIC     value INT,
# MAGIC     data_hash INT
# MAGIC )
# MAGIC TBLPROPERTIES (
# MAGIC     delta.enableChangeDataFeed = true,
# MAGIC     delta.enableDeletionVectors = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO etl_framework_mvp.scd.source_table VALUES 
# MAGIC ('1', 'Alice', 10, 100),
# MAGIC ('2', 'Bob', 20, 200),
# MAGIC ('3', 'Charlie', 30, 300),
# MAGIC ('4', 'Charlie', 40, 400),
# MAGIC ('5', 'Charlie', 30, 500);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE etl_framework_mvp.scd.source_table SET name = 'Beyza' WHERE id = '2';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM etl_framework_mvp.scd.source_table WHERE id = '3';