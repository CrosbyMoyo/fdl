# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

from datetime import datetime, timezone

load_timestamp = datetime.now(timezone.utc)

# COMMAND ----------

dbutils.widgets.text(
    name='metadata_filename',
    defaultValue='',
    label='1 - metadata_filename'
)

# COMMAND ----------

metadata_filename = dbutils.widgets.get('metadata_filename')
logger.log.info(f'Widget: metadata_filename = "{metadata_filename}"')

# COMMAND ----------

metadata = GoldMetadataYaml(
    file_path = f'./metadata/{metadata_filename}',
    slv_catalog = env_vars.silver_catalog,
    gld_catalog = env_vars.gold_catalog
)

# COMMAND ----------

spark.sql(
    f'''
        SELECT
            -- Keys
            f.order_number AS internal_order

            -- Foreign Keys 
            ,f.created_on AS date_key
            ,{metadata.get_fkey_ddl(['f.company_code'])} AS company_code_skey 
            ,{metadata.get_fkey_ddl(['f.subtype_name'])} AS subtype_name_skey
            ,f.order_currency AS currency_skey 
            ,f.plant AS plant_skey
            ,{metadata.get_fkey_ddl(['f.controlling_area','f.profit_center'])} AS profit_center_skey
            ,{metadata.get_fkey_ddl(['p.line_of_business_1'])} AS line_of_business_skey 
            
            -- Payload
            ,f.object_number
            ,f.order_type
            ,f.order_category
            ,f.ledger
            ,f.fiscal_year
            ,f.budget_local
            ,f.allocated_local
            ,f.budget_usd
            ,f.actuals_usd
            ,f.committed_usd
            ,f.actuals_local
            ,f.committed_local

            -- Metadata
            ,0 AS __etl_fprint
            ,'{load_timestamp}' AS __etl_load_timestamp
            ,True AS __etl_is_active
            ,False AS __etl_is_deleted
        FROM 
            {metadata.alias2src('cc')} AS f
        INNER JOIN
            {metadata.alias2src('pc')} AS p ON 
            f.profit_center = p.profit_center
    '''
).createOrReplaceTempView('gold_table')

# COMMAND ----------

spark.sql(
    f'''
        INSERT OVERWRITE {metadata.dest_3partname(True)}
        (
            -- keys
            internal_order
            ,date_key
            ,company_code_skey
            ,subtype_name_skey
            ,profit_center_skey
            ,line_of_business_skey
            ,currency_skey
            ,plant_skey          

            -- payload
            ,object_number
            ,order_type
            ,order_category
            ,ledger
            ,fiscal_year

            -- measures
            ,budget_local
            ,allocated_local
            ,budget_usd
            ,actuals_usd
            ,committed_usd
            ,actuals_local
            ,committed_local

            -- metadata
            ,__etl_fprint
            ,__etl_load_timestamp
            ,__etl_is_active
            ,__etl_is_deleted
        )
        SELECT
            -- Foreign Keys 
            g.internal_order
            ,g.date_key
            ,g.company_code_skey
            ,g.subtype_name_skey
            ,g.profit_center_skey
            ,g.line_of_business_skey
            ,g.currency_skey
            ,g.plant_skey
            
            -- PAYLOAD
            ,g.object_number
            ,g.order_type
            ,g.order_category
            ,g.ledger
            ,g.fiscal_year

            -- measures
            ,g.budget_local
            ,g.allocated_local
            ,g.budget_usd
            ,g.actuals_usd
            ,g.committed_usd
            ,g.actuals_local
            ,g.committed_local

            --metadata
            ,0
            ,g.__etl_load_timestamp
            ,g.__etl_is_active
            ,g.__etl_is_deleted

        FROM
            gold_table g
    '''
)