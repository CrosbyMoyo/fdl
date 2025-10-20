# Databricks notebook source
import dlt
from pyspark.sql import functions as F

def create_table_functions(table_name, rules_dict, intable_schema, intable_name, outtable_schema, outtable_name, quarantine_table_name, expectation_action):

    # Check the expectation action and create the appropriate DLT table
    if expectation_action.lower() == "drop":
        @dlt.table(name=f"{intable_schema}_{intable_name}")
        @dlt.expect_all(rules_dict)
        def validation():
            # Load data for the current table and add a column to indicate quarantined rows
            return spark.table(f"etl_framework_mvp.{intable_schema}.{intable_name}").withColumn("is_quarantined", ~F.expr(" AND ".join(rules_dict.values())))
    
    elif expectation_action.lower() == "fail":
        @dlt.table(name=f"{intable_schema}_{intable_name}")
        @dlt.expect_all_or_fail(rules_dict)
        def validation():
            # Load data for the current table and add a column to indicate quarantined rows
            return spark.table(f"etl_framework_mvp.{intable_schema}.{intable_name}").withColumn("is_quarantined", ~F.expr(" AND ".join(rules_dict.values())))
    else:
        raise ValueError("Not a valid expectation action")
        
    # Create a DLT table for valid data
    @dlt.table(name=f"{outtable_schema}_{outtable_name}")
    def valid_data():
        return spark.table(f"LIVE.{intable_schema}_{intable_name}").filter(F.col("is_quarantined") == False)
    
    # Create a DLT table for quarantined data
    @dlt.table(name=f"{outtable_schema}_{outtable_name}_quarantine")
    def quarantine_data():
        return spark.table(f"LIVE.{intable_schema}_{intable_name}").filter(F.col("is_quarantined") == True)

# COMMAND ----------

# Read the data quality rules from the Delta table
dq_rules = spark.read.format("delta").table("etl_framework_mvp.silver.rules_demo")

# Collect unique table names from the rules
unique_tables = dq_rules.select("table", "schema", "target_schema").distinct().collect()

# Generate functions dynamically for each table
for table_row in unique_tables:
    table_name = table_row["table"]
    intable_schema = table_row["schema"]
    intable_name = table_name
    outtable_schema = table_row["target_schema"]
    outtable_name = table_name
    quarantine_table_name = f"quarantine_{table_name}"

    # Filter rules for the current table
    table_rules = dq_rules.filter(F.col('table') == table_name)
    rule_dict = {row["expectation"]: row["expectation_sql"] for row in table_rules.select("expectation", "expectation_sql").collect()}

    # Get distinct expectation actions for the current table
    table_expectation_actions = table_rules.select("expectation_action").distinct()

    # Ensure there is only one expectation action for the table
    if table_expectation_actions.count() != 1:
        raise Exception(f"Multiple expectation actions found for table {table_name}")

    # Get the expectation action
    expectation_action = table_expectation_actions.collect()[0]["expectation_action"]

    # Create table functions dynamically
    create_table_functions(table_name, rule_dict, intable_schema, intable_name, outtable_schema, outtable_name, quarantine_table_name, expectation_action)