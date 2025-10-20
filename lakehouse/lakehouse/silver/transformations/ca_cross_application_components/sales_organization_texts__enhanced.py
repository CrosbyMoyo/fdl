# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.tvkot.yaml"
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", True)}'

# COMMAND ----------

sales_org_cte = spark.sql(f'''
    SELECT 
        so.sales_organization,
        so.sales_organization_description,
        so.__etl_keys_fprint,
        xxhash64(
            so.sales_organization_description
        ) AS __etl_row_fprint,
        so.__etl_effective_from,
        so.__etl_effective_to,
        so.__etl_is_active,
        so.__etl_is_deleted
    FROM {env_vars.silver_catalog}.ca_cross_application_components_staging.sales_organization_texts__casted AS so
    WHERE language_key = "E";                    
''').createOrReplaceTempView('sales_org_cte')

# COMMAND ----------

merge_result = spark.sql(f'''
    MERGE INTO {dest_tablename} AS target
    USING sales_org_cte AS source
        ON target.__etl_keys_fprint = source.__etl_keys_fprint
    WHEN MATCHED THEN
        UPDATE SET 
            target.sales_organization = source.sales_organization,
            target.sales_organization_description = source.sales_organization_description,
            target.__etl_row_fprint = source.__etl_row_fprint,
            target.__etl_effective_from = source.__etl_effective_from,
            target.__etl_effective_to = source.__etl_effective_to,
            target.__etl_is_active = source.__etl_is_active,
            target.__etl_is_deleted = source.__etl_is_deleted
    WHEN NOT MATCHED THEN
        INSERT (
            sales_organization,
            sales_organization_description,
            __etl_keys_fprint,
            __etl_row_fprint,
            __etl_effective_from,
            __etl_effective_to,
            __etl_is_active,
            __etl_is_deleted
        ) VALUES (
            source.sales_organization,
            source.sales_organization_description,
            source.__etl_keys_fprint,
            source.__etl_row_fprint,
            source.__etl_effective_from,
            source.__etl_effective_to,
            source.__etl_is_active,
            source.__etl_is_deleted
        );
''')

# COMMAND ----------

logger.log.info(f'Merge: {dest_tablename} {merge_result.toPandas().head(1)}')