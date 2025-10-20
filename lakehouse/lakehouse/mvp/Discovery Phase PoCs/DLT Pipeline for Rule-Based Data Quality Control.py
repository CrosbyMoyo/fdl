# Databricks notebook source
# MAGIC %md
# MAGIC This pipeline is able to quarantine records that violate records as well as specifying in the quarantine table the reason for failure. If two rules are violated for the same record, then both reasons are specicified in the 'violations' column in the qurantine table. 

# COMMAND ----------

# Define the schema
schema = """
    catalog STRING COMMENT 'The catalog that the table exists in',
    schema STRING COMMENT 'The schema that the table exists in',
    table STRING COMMENT 'The table the data quality expectation applies to',
    expectation STRING COMMENT 'The name of the expectation',
    expectation_sql STRING COMMENT 'The SQL check for the expectation',
    expectation_action STRING COMMENT 'The action to be taken based on the expectation',
    target_schema STRING COMMENT 'The table to which the input table will be written to'
"""

# Create an empty DataFrame with the schema
df = spark.createDataFrame([], schema)

# Create or replace the temporary view
df.createOrReplaceTempView("rules")

# COMMAND ----------

# Create a DataFrame with the new rules (including the new row)
new_rules = spark.createDataFrame(
    [
        ('dev', 'brz', 'kna1_test4', 'geography_not_null', 'MCOD3 IS NOT NULL', 'fail', 'sl'),
        ('dev', 'brz', 'ska1_test1', 'xbilk_not_null', 'XBILK IS NOT NULL', 'DROP', 'sl'),
        ('dev', 'brz', 'knvp_test1', 'vkorg_is_not_null', 'VKORG IS NOT NULL', 'DROP', 'sl'),
        ('dev', 'brz', 'kna1_test4', 'land1_is_not_null', 'LAND1 IS NOT NULL', 'DROP', 'sl')
    ],
    ['catalog', 'schema', 'table', 'expectation', 'expectation_sql', 'expectation_action', 'target_schema']
)

# Insert the new rules into the 'rules' table
new_rules.write.mode('overwrite').option("mergeSchema", "true").saveAsTable('etl_framework_mvp.silver.rules')

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# Load data quality rules from the metadata table
dq_rules = spark.sql("SELECT * FROM etl_framework_mvp.silver.rules")

# Collect unique table names from the rules
unique_tables = dq_rules.select("table").distinct().collect()

def create_table_functions(table_name, table_rules, intable_name, outtable_name, quarantine_table_name):
    @dlt.table(name=f"{outtable_name}_dlt")
    def table_function():
        # Load data for the current table
        data = spark.table(f"etl_framework_mvp.bronze.{intable_name}")

        # Initialize a column for violations
        data = data.withColumn("violations", F.lit(None).cast("array<string>"))

        # Apply rules dynamically for this table
        for rule in table_rules.collect():
            expectation = rule["expectation"]
            expectation_sql = rule["expectation_sql"]
            action = rule["expectation_action"]

            print(f"Applying rule '{expectation}' on table '{intable_name}'")

            # Mark rows violating the rule
            data = data.withColumn(
                "violations",
                F.when(~F.expr(expectation_sql), 
                       F.when(F.col("violations").isNull(), F.array(F.lit(expectation)))  # Initialize array if null
                       .otherwise(F.concat(F.col("violations"), F.array(F.lit(expectation))))  # Append to array
                      )
                .otherwise(F.col("violations"))
            )
            # Filter valid rows if the action is DROP
            # if action.upper() == "DROP":
            #     data = data.filter(F.expr(expectation_sql) | F.col("violations").isNotNull())

        # Separate valid and invalid data
        quarantine_data = data.filter(F.col("violations").isNotNull())
        valid_data = data.filter(F.col("violations").isNull())

        # Persist the quarantine data in a temporary view for this table
        quarantine_view_name = f"quarantine_{outtable_name}_temp"
        if quarantine_data.count() == 0:  # Use count() to check for empty DataFrame
            # Create an empty DataFrame with the same schema as the original
            quarantine_data = spark.createDataFrame([], schema=data.schema)
        quarantine_data.createOrReplaceTempView(quarantine_view_name)

        # Return the valid data to Delta Live Tables
        return valid_data

    @dlt.table(name=quarantine_table_name)
    def quarantine_function():
        # Handle missing temporary views
        quarantine_view_name = f"quarantine_{outtable_name}_temp"
        try:
            return spark.sql(f"SELECT * FROM {quarantine_view_name}")
        except:
            # Return an empty DataFrame if the view does not exist
            empty_schema = spark.createDataFrame([], data.schema)
            return empty_schema

# Generate functions dynamically for each table
for table_row in unique_tables:
    table_name = table_row["table"]
    intable_name = f"brz_{table_name}"
    outtable_name = f"sl_{table_name}"
    quarantine_table_name = f"quarantine_{table_name}"

    # Filter rules for the current table
    table_rules = dq_rules.filter(F.col('table') == table_name)

    # Create table functions dynamically
    create_table_functions(table_name, table_rules, intable_name, outtable_name, quarantine_table_name)




# COMMAND ----------

# MAGIC %md
# MAGIC @dlt.expect_or_drop("valid_geography", "MCOD3 IS NOT NULL")
# MAGIC
# MAGIC expectation = rule["expectation"]
# MAGIC expectation_sql = rule["expectation_sql"]
# MAGIC action = rule["expectation_action"]
# MAGIC
# MAGIC If action.upper() == DROP
# MAGIC @dlt.expect_or_drop("expectation", "expectation_sql")
# MAGIC
# MAGIC elif action.upper() = FAIL
# MAGIC @dlt.expect_or_fail("expectation", "expectation_sql")
# MAGIC
# MAGIC
# MAGIC Separate into rules_drop and rules_fail first so you don't have to do if and elif