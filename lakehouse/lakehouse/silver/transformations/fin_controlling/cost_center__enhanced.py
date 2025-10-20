# Databricks notebook source
# MAGIC %md
# MAGIC ## CSKS Bronze to Silver
# MAGIC
# MAGIC Generic Notebook moves the `{bronze}.fivetran_s4p.csks` data to a staging table defined in the metadata.
# MAGIC
# MAGIC This notebook further refines that data, and adds the payload fingerprint.  Then merges the data into `{silver}.fin_controlling.cost_center_master` table.

# COMMAND ----------

# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.csks.yaml"
logger.log.info(f'"metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

source_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("casted", include_schemaversion=True)}'

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", include_schemaversion=True)}'

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

ccm_enhanced = spark.sql(f'''
    SELECT
        src.client,
        src.controlling_area,
        src.cost_center,
        d.cost_center_description_long AS cost_center_description,
        src.valid_from,
        src.valid_to,
        src.company_code,
        -- Q: why does level_9_node_text appear twice? :-?
        -- And why wasn't this coalesced between Bronze and cleaned?
        COALESCE(h.level_9_node_text, h.level_9_node_text, '') AS budget_holder,
        src.cost_center_category,
        cd.cost_center_category_description,
        src.profit_center,
        src.department,
        src.__etl_keys_fprint,
        -- Q: why is level_9_node_text coalesced above, but not here?
        xxhash64({row_fprint_ddl}, d.cost_center_description_long, h.level_9_node_text, cd.cost_center_category_description) AS __etl_row_fprint,
        src.__etl_effective_from,
        src.__etl_effective_to,
        src.__etl_is_active,
        src.__etl_is_deleted
    FROM
        {source_tablename} AS src
        LEFT JOIN {env_vars.silver_catalog}.fin_controlling.cost_center_description AS d
            ON src.cost_center = d.cost_center
            AND src.controlling_area = d.controlling_area
            AND src.valid_to = d.valid_to
        LEFT JOIN {env_vars.silver_catalog}.fin_controlling.cost_center_category_description AS cd
            ON src.cost_center_category = cd.cost_center_category
        LEFT JOIN {env_vars.silver_catalog}.fin_controlling.cost_center_hierarchy as h
            ON src.cost_center = h.cost_center_hierarchy_node
            AND h.hierarchy_id = "BH";
''')

ccm_enhanced.createOrReplaceTempView('ccm_enhanced')

# COMMAND ----------

merge_result = spark.sql(f'''
    MERGE INTO {dest_tablename} AS tgt
    USING ccm_enhanced AS src
        ON tgt.__etl_keys_fprint = src.__etl_keys_fprint
    WHEN MATCHED THEN
        UPDATE SET
            {match_cols_ddl},
            tgt.cost_center_description = src.cost_center_description,
            tgt.cost_center_category_description = src.cost_center_category_description,
            tgt.budget_holder = src.budget_holder,
            tgt.__etl_row_fprint = src.__etl_row_fprint,
            tgt.__etl_effective_from = src.__etl_effective_from,
            tgt.__etl_effective_to = src.__etl_effective_to,
            tgt.__etl_is_active = src.__etl_is_active,
            tgt.__etl_is_deleted = src.__etl_is_deleted
    WHEN NOT MATCHED THEN
        INSERT (
            {insert_cols_tgt_ddl},
            cost_center_description,
            cost_center_category_description,
            budget_holder
        )
        VALUES (
            {insert_cols_src_ddl},
            src.cost_center_description,
            src.cost_center_category_description,
            src.budget_holder
        );
''')

# COMMAND ----------

logger.log.info(f'Merge: {dest_tablename} {merge_result.toPandas().head(1)}')