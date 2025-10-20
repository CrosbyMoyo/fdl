# Databricks notebook source
# MAGIC %md
# MAGIC ## MARA Bronze to Silver
# MAGIC
# MAGIC Generic Notebook moves the `{bronze}.fivetran_s4p.mara` data to a staging table defined in the metadata.
# MAGIC
# MAGIC This notebook further refines that data, and adds the payload fingerprint. Then merges the data into `{silver}.sl_extended_warehouse_management.material_master` table.

# COMMAND ----------

# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

# change the yaml file destination
metadata_filename = "silver.mara.yaml"
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

casted_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("casted", True)}'
dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", True)}'

# COMMAND ----------

# get the columns to xxhash64
row_fprint_ddl = metadata.get_payload_columns_ddl("src.")
# get the columns to MATCH
match_cols_ddl = metadata.get_update_set_ddl("src.", "tgt.")
# get the target columns to INSERT
insert_cols_tgt_ddl = metadata.get_insert_ddl()
# get the source columns to INSERT
insert_cols_src_ddl = metadata.get_insert_ddl("src.")

# COMMAND ----------

merge_result = spark.sql(f'''    
    WITH mara AS (
        SELECT 
            src.client,
            src.material_number,
            src.material_type,
            src.industry_sector,
            src.material_group,
            src.base_unit_of_measure,
            src.labor_office,
            src.volume,
            src.division,
            src.length,
            src.product_hierarchy,
            src.external_material_group,
            src.transportation_group,
            src.manufacturer_book_part_number,
            RANK() OVER (PARTITION BY src.material_group ORDER BY src.material_number) AS ranking,
            src.__etl_keys_fprint,
            xxhash64({row_fprint_ddl}, ranking) AS __etl_row_fprint,
            src.__etl_effective_from,
            src.__etl_effective_to,
            src.__etl_is_active,
            src.__etl_is_deleted
        FROM {casted_tablename} AS src
    )

    MERGE INTO {dest_tablename} AS tgt
    USING mara AS src
        ON 
            tgt.client = src.client
                AND 
            tgt.material_number = src.material_number
    WHEN MATCHED THEN
        UPDATE SET 
            {match_cols_ddl},
            tgt.ranking = src.ranking,
            tgt.__etl_row_fprint = src.__etl_row_fprint,
            tgt.__etl_effective_from = src.__etl_effective_from,
            tgt.__etl_effective_to = src.__etl_effective_to,
            tgt.__etl_is_active = src.__etl_is_active,
            tgt.__etl_is_deleted = src.__etl_is_deleted
    WHEN NOT MATCHED THEN
        INSERT (
            {insert_cols_tgt_ddl},
            ranking
        ) VALUES (
            {insert_cols_src_ddl},
            src.ranking
        );
''')

# COMMAND ----------

logger.log.info(f'Merge: {dest_tablename} {merge_result.toPandas().head(1)}')