# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

# change the yaml file destination
metadata_filename = "silver.scal_tt_date.yaml"
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

casted_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("casted", True)}'
dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", True)}'

# COMMAND ----------

# get the columns to xxhash64
row_fprint_ddl = metadata.get_payload_columns_ddl("c.")
# get the columns to MATCH
match_cols_ddl = metadata.get_update_set_ddl("source.", "target.")
# get the target columns to INSERT
insert_cols_tgt_ddl = metadata.get_insert_ddl()
# get the source columns to INSERT
insert_cols_src_ddl = metadata.get_insert_ddl("source.")

# COMMAND ----------

casted = spark.sql(f'''
    SELECT
        -- keys
        {env_vars.silver_catalog}.ca_cross_application_components.extract_sap_date(c.calendar_date) AS calendar_date

        -- payload
        ,c.calendar_year
        ,c.calendar_month
        ,c.calendar_quarter
        ,c.calendar_week
        ,c.calendar_day
        ,c.week_day
        ,c.year_week
        ,c.year_quarter
        ,c.year_month
        ,{env_vars.silver_catalog}.ca_cross_application_components.extract_sap_date(c.first_day_of_month) AS first_day_of_month
        ,{env_vars.silver_catalog}.ca_cross_application_components.extract_sap_date(c.last_day_of_month) AS last_day_of_month
        ,c.half_year
        ,{env_vars.silver_catalog}.ca_cross_application_components.extract_sap_date(c.first_day_of_week_date) AS first_day_of_week_date
        ,c.calendar_day_of_year
        ,c.year_day

        -- metadata
        ,c.__etl_keys_fprint
        ,c.__etl_effective_from
        ,c.__etl_effective_to
        ,c.__etl_is_active
        ,c.__etl_is_deleted
        ,xxhash64(
            {row_fprint_ddl}
        ) AS __etl_row_fprint
    FROM {casted_tablename} AS c;          
''').createOrReplaceTempView("casted")

# COMMAND ----------

merge_result = spark.sql(f'''
    MERGE INTO {dest_tablename} AS target
    USING casted AS source
        ON target.__etl_keys_fprint = source.__etl_keys_fprint
    WHEN MATCHED THEN
        UPDATE SET
            {match_cols_ddl},
            target.__etl_row_fprint = source.__etl_row_fprint,
            target.__etl_effective_from = source.__etl_effective_from,
            target.__etl_effective_to = source.__etl_effective_to,
            target.__etl_is_active = source.__etl_is_active,
            target.__etl_is_deleted = source.__etl_is_deleted
    WHEN NOT MATCHED THEN
        INSERT (
            {insert_cols_tgt_ddl}
        ) VALUES (
            {insert_cols_src_ddl}
        );
''')

# COMMAND ----------

logger.log.info(f'Merge: {dest_tablename} {merge_result.toPandas().head(1)}')