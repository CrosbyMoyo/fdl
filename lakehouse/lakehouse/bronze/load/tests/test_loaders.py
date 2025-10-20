# Databricks notebook source
# MAGIC %run ../loaders

# COMMAND ----------

# Create a test table with Change Data Feed (CDF) enabled
spark.sql("CREATE SCHEMA IF NOT EXISTS vivid_nward_brz.test")

# Bronze Hop 1 - Replica
spark.sql("""
CREATE OR REPLACE TABLE vivid_nward_brz.test.test_table__replica (
    id INT,
    name STRING,
    age INT
)
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# Bronze Hop 2 - CDC 
spark.sql("""
CREATE OR REPLACE TABLE vivid_nward_brz.test.test_table__cdc (
    id INT,
    name STRING,
    age INT,
    __etl_source_operation STRING,
    __etl_bronze_timestamp TIMESTAMP
)
""")

# Bronze Hop 3 - Soft Delete 
spark.sql("""
CREATE OR REPLACE TABLE vivid_nward_brz.test.test_table (
    id INT,
    name STRING,
    age INT,
    __etl_bronze_timestamp TIMESTAMP,
    __etl_silver_timestamp TIMESTAMP,
    __etl_is_deleted BOOLEAN
)
""")

# COMMAND ----------

listener = ListeningTom()
spark.streams.addListener(listener)

# COMMAND ----------

cdf_transformer = ChangeDataCaptureTransformer()

cdf_loader = StreamOrchestrator(
    source_table="vivid_nward_brz.test.test_table__replica",
    target_table="vivid_nward_brz.test.test_table__cdc",
    checkpoint_location="abfss://vivid@vividstoragedev.dfs.core.windows.net/metadata/offsets/vivid_nward_brz.test.test_table__cdc",
    read_kwargs={'readChangeFeed': True},
    spark=spark
)

cdf_loader.add(cdf_transformer)

# COMMAND ----------

brz_processor = SoftDeleteProcessor(primary_key=['id'], source_table="vivid_nward_brz.test.test_table__cdc", target_table="vivid_nward_brz.test.test_table")

brz_loader = StreamOrchestrator(
    source_table="vivid_nward_brz.test.test_table__cdc",
    target_table="vivid_nward_brz.test.test_table",
    checkpoint_location="abfss://vivid@vividstoragedev.dfs.core.windows.net/metadata/offsets/vivid_nward_brz.test.test_table",
    spark=spark
)

# COMMAND ----------

# hop 1
spark.sql("""
    INSERT INTO vivid_nward_brz.test.test_table__replica (id, name, age) VALUES
    (1, 'Antonio Banderes', 30),
    (2, 'John Wayne', 25),
    (3, 'Wart Simpson', 35)
""")

# hop 2 
cdf_loader.run()
display(spark.table('vivid_nward_brz.test.test_table__cdc'))

# hop 3
brz_loader.run(brz_processor)
display(spark.table('vivid_nward_brz.test.test_table'))

# COMMAND ----------

# hop 1 
spark.sql("""
DELETE FROM vivid_nward_brz.test.test_table__replica WHERE id = 1
""")

# hop 2
cdf_loader.run()
display(spark.table('vivid_nward_brz.test.test_table__cdc'))

# hop 3
brz_loader.run(brz_processor)
display(spark.table('vivid_nward_brz.test.test_table'))

# COMMAND ----------

# hop 1 
spark.sql("""
UPDATE vivid_nward_brz.test.test_table__replica
SET name = 'El Warto', age = 31
WHERE id = 3
""")

# hop 2
cdf_loader.run()
display(spark.table('vivid_nward_brz.test.test_table__cdc'))

# hop 3
brz_loader.run(brz_processor)
display(spark.table('vivid_nward_brz.test.test_table'))

# COMMAND ----------

# Clean up 
spark.sql("DROP SCHEMA IF EXISTS vivid_nward_brz.test CASCADE")
dbutils.fs.rm(cdf_loader.checkpoint_location, True)
dbutils.fs.rm(brz_loader.checkpoint_location, True)
spark.streams.removeListener(listener)