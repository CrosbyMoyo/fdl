# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_general_ledger_presentation.fact_commentary
    AS
    SELECT
        fc.comment_id,
        fc.comment,
        fc.commenter_email,
        fc.commenter_name,
        fc.region,
        fc.country,
        fc.feedback_ind,
        fc.group_code,
        fc.month,
        fc.page,
        fc.report,
        fc.soft_delete,
        fc.subject,
        fc.timestamp,
        fc.year
    FROM
        {env_vars.gold_catalog}.fin_general_ledger.fact_commentary AS fc;
''')
