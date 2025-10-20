# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.zrtr_prctr_tab.yaml"
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

source_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("casted", True)}'
dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", True)}'

# COMMAND ----------

prctr_sub = spark.sql(f'''
    SELECT 
        sub.profit_center,
        sub.division,
        sd.sales_division_description,
        sub.sales_organization,
        so.sales_organization_description,
        sub.distribution_channel,
        dc.distribution_channel_description,
        sub.__etl_keys_fprint,
        xxhash64(
            sub.profit_center, sd.sales_division_description, so.sales_organization_description, dc.distribution_channel_description
        ) AS __etl_row_fprint,
        sub.__etl_effective_from,
        sub.__etl_effective_to,
        sub.__etl_is_active,
        sub.__etl_is_deleted
    FROM {source_tablename} AS sub
    INNER JOIN {env_vars.silver_catalog}.ca_cross_application_components.sales_division_texts AS sd
        ON sub.division = sd.division
    INNER JOIN {env_vars.silver_catalog}.ca_cross_application_components.sales_organization_texts AS so
        ON sub.sales_organization = so.sales_organization
    INNER JOIN {env_vars.silver_catalog}.ca_cross_application_components.distribution_channel_texts AS dc
        ON sub.distribution_channel = dc.distribution_channel;                  
''').createOrReplaceTempView('prctr_sub')

# COMMAND ----------

merge_result = spark.sql(f'''
    MERGE INTO {dest_tablename} AS target
    USING prctr_sub AS source
        ON target.__etl_keys_fprint = source.__etl_keys_fprint
    WHEN MATCHED THEN
        UPDATE SET 
            target.profit_center = source.profit_center,
            target.division = source.division,
            target.sales_division_description = source.sales_division_description,
            target.sales_organization = source.sales_organization,
            target.sales_organization_description = source.sales_organization_description,
            target.distribution_channel = source.distribution_channel,
            target.distribution_channel_description = source.distribution_channel_description, 
            target.__etl_row_fprint = source.__etl_row_fprint,
            target.__etl_effective_from = source.__etl_effective_from,
            target.__etl_effective_to = source.__etl_effective_to,
            target.__etl_is_active = source.__etl_is_active,
            target.__etl_is_deleted = source.__etl_is_deleted
    WHEN NOT MATCHED THEN
        INSERT (
            profit_center,
            division,
            sales_division_description,
            sales_organization,
            sales_organization_description,
            distribution_channel,
            distribution_channel_description,
            __etl_keys_fprint,
            __etl_row_fprint,
            __etl_effective_from,
            __etl_effective_to,
            __etl_is_active,
            __etl_is_deleted
        ) VALUES (
            source.profit_center,
            source.division,
            source.sales_division_description,
            source.sales_organization,
            source.sales_organization_description,
            source.distribution_channel,
            source.distribution_channel_description,
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