# Databricks notebook source
# MAGIC %md
# MAGIC ## workspaces view
# MAGIC
# MAGIC Holds a hard-coded list of the workspace ID and the friendly name.  When new workspaces are created, they will need manually adding to this list (or automate with the CLI if you are bored and fancy the challenge)
# MAGIC
# MAGIC NB: the system tables record the workspace ID, but not the name - hence the need for this view.

# COMMAND ----------

# MAGIC %run ../../common/properties

# COMMAND ----------

spark.sql(f'''

    CREATE OR REPLACE VIEW {env_vars.general_catalog}.finops.workspaces
    AS
    SELECT
        w.workspace_id
        ,w.workspace_name
    FROM
        VALUES
            -- old world
            ('4142687418462502', 'dbw-uks-sapreporting-dev-1')
            ,('3122233307216791', 'dbw-uks-sapreporting-prd-1')
            -- new world
            ,('4177695357011526', 'vivid-dbx-dev')
            ,('4271780246052616', 'vivid-dbx-int')
            ,('194892457125280', 'vivid-dbx-prd')
        AS
            w(workspace_id, workspace_name);

''')