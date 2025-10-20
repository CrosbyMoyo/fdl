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
            -- Foreign Keys 
            f.year_month
            
            -- PAYLOAD
            ,f.special_item_code
            ,f.amount_mtd
            ,f.amount_ytd

            --metadata
            ,0 AS __etl_fprint
            ,'{load_timestamp}' AS __etl_load_timestamp
            ,True AS __etl_is_active
            ,False AS __etl_is_deleted
        FROM 
            {metadata.alias2src('si')} AS f
    '''
).createOrReplaceTempView('gold_table')

# COMMAND ----------

spark.sql(
    f'''
        INSERT OVERWRITE {metadata.dest_3partname(True)}
        (
            -- keys
            year_month

            -- payload
            ,special_item_code
            ,amount_mtd
            ,amount_ytd

            -- metadata
            ,__etl_fprint
            ,__etl_load_timestamp
            ,__etl_is_active
            ,__etl_is_deleted
        )
        SELECT
            -- Foreign Keys 
            g.year_month
            
            -- PAYLOAD
            ,g.special_item_code
            ,g.amount_mtd
            ,g.amount_ytd

            --metadata
            ,0
            ,g.__etl_load_timestamp
            ,g.__etl_is_active
            ,g.__etl_is_deleted

        FROM
            gold_table g
    '''
)

# COMMAND ----------

