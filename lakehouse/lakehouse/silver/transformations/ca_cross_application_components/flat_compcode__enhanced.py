# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.flat_compcode.yaml"
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

payload_cols = ['c.' + c['rename_to'] for c in metadata.yaml['column_transformations'] if c['column_role'] == "PAYLOAD"]

payload_cols_str = ', '.join(payload_cols)

# COMMAND ----------

source_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("casted", include_schemaversion=True)}'

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", include_schemaversion=True)}'

# COMMAND ----------

pc_cc = spark.sql(f'''
    SELECT 
        c.vivo_group,
        c.geo_region,
        c.currency,
        c.region_alt_1,
        c.entity_grouping_level_0,
        c.entity_grouping_level_1,
        c.entity_grouping_level_top,
        c.entity_grouping_level_3_vp_reporting,
        c.entity_grouping_level_2_geographical,
        c.display_name,
        c.company_code,
        c.reporting_entity,
        c.central_credit_country_grouping,
        c.planning_company_code,
        c.reporting_entity_ri,
        c.__etl_keys_fprint,
        xxhash64(
            {payload_cols_str}
        ) AS __etl_row_fprint,
        c.__etl_effective_from,
        c.__etl_effective_to,
        c.__etl_is_active,
        c.__etl_is_deleted
    FROM {source_tablename} AS c;                   
''')
pc_cc.createOrReplaceTempView('1pc_cc')

# COMMAND ----------

merge_result = spark.sql(f'''
    MERGE INTO {dest_tablename} AS target
    USING 1pc_cc AS source
        ON target.__etl_keys_fprint = source.__etl_keys_fprint
    WHEN MATCHED THEN
        UPDATE SET 
            target.vivo_group = source.vivo_group,
            target.geo_region = source.geo_region,
            target.currency = source.currency,
            target.region_alt_1 = source.region_alt_1,
            target.entity_grouping_level_0 = source.entity_grouping_level_0,
            target.entity_grouping_level_1 = source.entity_grouping_level_1,
            target.entity_grouping_level_top = source.entity_grouping_level_top,
            target.entity_grouping_level_3_vp_reporting = source.entity_grouping_level_3_vp_reporting,
            target.entity_grouping_level_2_geographical = source.entity_grouping_level_2_geographical,
            target.display_name = source.display_name,
            target.company_code = source.company_code,
            target.reporting_entity = source.reporting_entity,
            target.central_credit_country_grouping = source.central_credit_country_grouping,
            target.planning_company_code = source.planning_company_code,
            target.reporting_entity_ri = source.reporting_entity_ri,
            target.__etl_row_fprint = source.__etl_row_fprint,
            target.__etl_effective_from = source.__etl_effective_from,
            target.__etl_effective_to = source.__etl_effective_to,
            target.__etl_is_active = source.__etl_is_active,
            target.__etl_is_deleted = source.__etl_is_deleted
    WHEN NOT MATCHED THEN
        INSERT (
            vivo_group,
            geo_region,
            currency,
            region_alt_1,
            entity_grouping_level_0,
            entity_grouping_level_1,
            entity_grouping_level_top,
            entity_grouping_level_3_vp_reporting,
            entity_grouping_level_2_geographical,
            display_name,
            company_code,
            reporting_entity,
            central_credit_country_grouping,
            planning_company_code,
            reporting_entity_ri,
            __etl_keys_fprint,
            __etl_row_fprint,
            __etl_effective_from,
            __etl_effective_to,
            __etl_is_active,
            __etl_is_deleted
        ) VALUES (
            source.vivo_group,
            source.geo_region,
            source.currency,
            source.region_alt_1,
            source.entity_grouping_level_0,
            source.entity_grouping_level_1,
            source.entity_grouping_level_top,
            source.entity_grouping_level_3_vp_reporting,
            source.entity_grouping_level_2_geographical,
            source.display_name,
            source.company_code,
            source.reporting_entity,
            source.central_credit_country_grouping,
            source.planning_company_code,
            source.reporting_entity_ri,
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