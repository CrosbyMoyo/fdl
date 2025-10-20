# Databricks notebook source
from pyspark.sql.functions import col, when
def get_constraints(table_name, expectation_action):
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

import dlt
import pyspark.sql.functions as F

def load_table_to_bronze(table_name):
    """
        Loads delta tables into bronze dlt.
        
        Parameters:
            table_name(str): The table name to be loaded into bronze dlt.
        
        Return:
            dlt table: The bronze dlt tables with the name table_name and is_quarantined column. 
    """

    rules_drop = get_constraints(table_name, expectation_action='DROP')
    rules_fail = get_constraints(table_name, expectation_action='FAIL')
    

    @dlt.table(name=f"brz_{table_name.lower()}_dlt",comment=f"This is a bronze table brz_{table_name.lower()}_dlt")
    @dlt.expect_all_or_fail(rules_fail)
    @dlt.expect_all(rules_drop)
    def to_bronze():
        # check if the rules_drop is empty
        if len(rules_drop)>0:
            return(
                spark.readStream
                    .option("readChangeFeed", "true")
                    .table(f"etl_framework_mvp.bronze.brz_{table_name.lower()}_test1").withColumn("is_quarantined", ~F.expr(" AND ".join(rules_drop.values())))
            )
        # if the rules_drop is empty dict, then return the table with is_quarantined column as False
        else:
            return(
                spark.readStream
                    .format("delta")
                    .option("readChangeFeed", "true")
                    .table(f"etl_framework_mvp.bronze.brz_{table_name.lower()}_test1").withColumn("is_quarantined", F.lit(False))
            )
            
    
    @dlt.table(name=f"quarantine_{table_name.lower()}",comment=f"This is a quarantine table quarantine_{table_name.lower()}")
    def quarantine():
        return(
            dlt.read_stream(f"brz_{table_name.lower()}_dlt").filter(F.col("is_quarantined") == True)
              )


# COMMAND ----------

df = spark.table('etl_framework_mvp.staging.staging_table_test')
distinct_source_tables = df.select("source_table").distinct()
tables = distinct_source_tables.collect()


for table in tables:
    table_name = table['source_table']
    load_table_to_bronze(table_name)
    

# COMMAND ----------


          
def load_table_to_silver(table_name):
    """
        Loads bronze dlts into silver dlts by applying constraints.

        Parameters:
            table_name(str): The metadata table name to be loaded into silver dlt.  

        Returns:
            dlt table: The silver dlt tables with applied constraints.      
    """


    @dlt.table(name=f"slv_{table_name.lower()}_dlt", comment="Silver DLT")
    def to_silver():
        return(
            dlt.read_stream(f"brz_{table_name.lower()}_dlt").filter(F.col("is_quarantined") == False)
            )
    dlt.create_streaming_table(f"slv_cdf_{table_name}")

    # Apply changes to the silver table using dlt.apply_changes within the DLT query function
    dlt.apply_changes(
        target=f"slv_cdf_{table_name}",        # Define the target silver table
        source=f"slv_{table_name}_dlt",                # Use the filtered source data
        keys=[f"pk_{table_name}"],         # Specify the key columns
        sequence_by=col("_commit_timestamp"), # Order by the commit timestamp
        except_column_list=["_change_type", "_commit_version", "_commit_timestamp"],  # Exclude metadata columns
        apply_as_deletes=F.expr("_change_type = 'delete'"), # Handle delete operations
        stored_as_scd_type=2       # Use SCD Type 1 (overwrite)
    )



# COMMAND ----------

for table in tables:
    table_name = table['source_table']
    load_table_to_silver(table_name)

# COMMAND ----------

# dlt.create_streaming_table("slv_cdf_kna1")

# dlt.apply_changes(
#   target = "slv_cdf_kna1",
#   source = "brz_kna1_dlt",
#   keys = ["KUNNR"],
#   sequence_by = col('_commit_version'),
#   except_column_list = ["_change_type", "_commit_version", "_commit_timestamp"],
#   apply_as_deletes = F.expr("_change_type = 'delete'"),
#   stored_as_scd_type = 1
# )


# COMMAND ----------

# @dlt.table
# def source_with_cdf():
#     return spark.readStream.option("readChangeFeed", "true").table("etl_framework_mvp.scd.source_table")

# dlt.create_streaming_table("target_table")

# dlt.apply_changes(
#     target="target_table",
#     source="source_with_cdf",
#     keys=["id"],
#     sequence_by=col("_commit_timestamp"),
#     except_column_list = ["_change_type", "_commit_version", "_commit_timestamp"],
#     apply_as_deletes=F.expr("_change_type = 'delete'"),
#     stored_as_scd_type=1
# )