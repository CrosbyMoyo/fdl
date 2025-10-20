# Databricks notebook source
# MAGIC %md
# MAGIC ## Validate: {Test Name}  
# MAGIC #### Purpose:
# MAGIC - {Briefly describe the business logic being tested. What behavior or rule does this query aim to validate?}
# MAGIC
# MAGIC #### Pass/Fail Criteria:
# MAGIC - **Pass Criteria:** {Define the conditions under which the test/s will be considered successful}
# MAGIC - **Fail Criteria:** {Define the conditions that would result in a test failure}
# MAGIC
# MAGIC #### Edge Cases & Exceptions:
# MAGIC - {If applicable describe edge cases or exceptions that are excluded from this test}

# COMMAND ----------

# MAGIC %run ../../common/properties

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

# Your SQL test should raise an exception if your test condition is not met, you can do this by making use of SQL assertions like this: 
# See: https://docs.databricks.com/aws/en/sql/language-manual/functions/assert_true
spark.sql(
    f'''
    SELECT 
        assert_true(1 = 1, 'This will not raise an exception')
        ,assert_true(1 = 2, 'This will raise an exception')
    '''
).display()

# COMMAND ----------

# Alternatively, you can write a query that should only return rows if your fail condition has been met then raise an exception if rows are returned
# This makes it easier to debug the issue because you can display the results
validation = spark.sql(
    f'''
    SELECT 
        1 = 2 AS validation_check
    '''
)

validation.display()

assert validation.count() == 0, 'This will raise an exception'