# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql.functions import md5, concat, col, current_timestamp, to_timestamp, lit, sha2, coalesce, expr, concat_ws, rand

# COMMAND ----------

def create_tables():
    """
    Creates the bronze tables for the tables listed in the metadata table.

    Returns:
        None

    """
    # read the metadata table as a dataframe and collect the distinct table names
    df = spark.table("etl_framework_mvp.metadata.staging_table")
    rows = df.select("source_system", "source_table", "primary_keys").dropDuplicates(["source_table"]).collect()

    # iterate through the metadata table rows and extract the table names and the primary keys
    for row in rows:
        source_system = row["source_system"]
        table_name = row["source_table"]
        primary_keys = row["primary_keys"].split(",")

    # drop the table to recreate if it already exists temproary approach for now and should be removed for future development
        spark.sql(f"""
                  DROP TABLE IF EXISTS etl_framework_mvp.bronze.brz_{table_name.lower()}
                  """)
        
    # the path in the ADLS where the data sits - with tools like fivetran coming in this wont be necessary but can be used for other sources.
        file_path = f'abfss://raw@stukssapreportingdev.dfs.core.windows.net/{source_system} / {table_name} /2024/11/25'
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
        print(f"Table is created: brz_{table_name.lower()}")
        etl_load_df.write\
            .option("overwriteSchema", "true")\
            .mode("overwrite")\
            .saveAsTable(f'etl_framework_mvp.bronze.brz_{table_name.lower()}')
    # enable cdf deletion vectors and row tracking
        spark.sql(f"""
                    ALTER TABLE etl_framework_mvp.bronze.brz_{table_name.lower()}
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

# COMMAND ----------

def update_tables_merge():
    """
    Updates the the tables that are in the staging metadata table by taking a new file from the ADLS.

    Returns:
        None
    """
    
    # read the staging metadata table and create rows according to distinct table names
    df = spark.table("etl_framework_mvp.metadata.staging_table")
    rows = df.select("source_system", "source_table", "primary_keys", "load_type").dropDuplicates(["source_table"]).filter(df["source_system"] == 'SAP').collect()

    # iterate through the staging table rows and extract the table names and the primary keys
    for row in rows:
        source_system = row["source_system"]
        table_name = row["source_table"]
        print(table_name)
        primary_keys = row["primary_keys"].split(",")
        load_type = row["load_type"]

        try:
            # the new file path from ADLS -> this can be made more dynamic according to the extraction logic
            file_path = f'abfss://raw@stukssapreportingdev.dfs.core.windows.net/SAP / {table_name} /2024/12/04'
            # read the new file from ADLS and create a source dataframe
            source_df = spark.read.format("parquet").load(file_path)
        except Exception as e:
            if "Path does not exist" in str(e):
                print(f"No new file found for {table_name}")
            else:
                print(f"Error: {e}")

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
        target_table_name = f'etl_framework_mvp.bronze.brz_{table_name.lower()}'
        delta_table = DeltaTable.forName(spark, target_table_name) 

        # aliases for merge
        source_alias = source_with_pk.alias("source")
        delta_table_alias = delta_table.alias("target")

        merge_condition = f"source.pk_{table_name.lower()} = target.pk_{table_name.lower()}"

        if load_type == 'incremental':
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
        changes_df = spark.read.format("delta").option("readChangeData", "true").option("startingVersion", latest_version).table(f"etl_framework_mvp.bronze.brz_{table_name.lower()}")

        if changes_df.isEmpty():
            print("~No updates~")
        else:
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