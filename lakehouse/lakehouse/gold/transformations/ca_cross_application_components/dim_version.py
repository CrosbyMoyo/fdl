# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TEMP VIEW temp_view AS
        SELECT 
            xxhash64('ACTUAL') AS version_skey, 
            'ACTUAL' AS version_id, 
            'Actual' AS version_description
        UNION ALL
        SELECT 
            xxhash64('TARGET') AS version_skey, 
            'TARGET' AS version_id, 
            'Target' AS version_description
         UNION ALL
        SELECT 
            xxhash64('PLAN') AS version_skey, 
            'PLAN' AS version_id, 
            'Plan' AS version_description           
''')

# COMMAND ----------

spark.sql(f'''
    WITH ftps AS (
        SELECT 
            xxhash64(f.plan_version) AS version_skey,
            f.plan_version AS version_id,
            'Plan' AS version_description
        FROM {env_vars.silver_catalog}.fin_finance.finance_transactions_plan AS f
            GROUP BY version_skey, version_id
    ),

    ftps_union AS (
            SELECT * FROM ftps
        UNION 
            SELECT * FROM temp_view
    )

    MERGE INTO {env_vars.gold_catalog}.ca_cross_application_components.dim_version AS target
    USING ftps_union AS source
        ON target.version_skey = source.version_skey
    WHEN MATCHED THEN 
        UPDATE SET
            target.version_id = source.version_id,
            target.version_description = source.version_description
    WHEN NOT MATCHED THEN
        INSERT (
            version_skey,
            version_id,
            version_description
        ) VALUES (
            source.version_skey,
            source.version_id,
            source.version_description
        );
''')