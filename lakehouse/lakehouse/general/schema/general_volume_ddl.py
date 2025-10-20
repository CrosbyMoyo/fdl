# Databricks notebook source
# MAGIC %md
# MAGIC ## general_volume_ddl
# MAGIC
# MAGIC Idempotent script to set up the volumes in the general catalog 

# COMMAND ----------

# MAGIC %run ../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE EXTERNAL VOLUME IF NOT EXISTS {env_vars.general_catalog}.artefacts.env
    LOCATION 'abfss://vivid@vividstorage{env_vars.env}.dfs.core.windows.net/vivid_general/env'
    COMMENT 'Volume containing environment variables, requirements and venv config files.'
''')

# COMMAND ----------

spark.sql(f'''
    CREATE EXTERNAL VOLUME IF NOT EXISTS {env_vars.general_catalog}.artefacts.wheels
    LOCATION 'abfss://vivid@vividstorage{env_vars.env}.dfs.core.windows.net/vivid_general/wheels'
    COMMENT 'Volume containing python wheels.'
''')