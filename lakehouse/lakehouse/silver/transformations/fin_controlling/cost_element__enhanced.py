# Databricks notebook source
# MAGIC %md
# MAGIC ## CSKU Bronze to Silver
# MAGIC
# MAGIC Generic Notebook moves the `{bronze}.fivetran_s4p.csku` data to a staging table defined in the metadata.
# MAGIC
# MAGIC This notebook further refines that data, and adds the payload fingerprint.  Then merges the data into `{silver}.fin_controlling.cost_element` table.

# COMMAND ----------

# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.csku.yaml"
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

casted_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("casted", True)}'
dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", True)}'

# COMMAND ----------

# get the columns to xxhash64
row_fprint_ddl = metadata.get_payload_columns_ddl("d.")
# get the columns to MATCH
match_cols_ddl = metadata.get_update_set_ddl("source.", "target.")
# get the target columns to INSERT
insert_cols_tgt_ddl = metadata.get_insert_ddl()
# get the source columns to INSERT
insert_cols_src_ddl = metadata.get_insert_ddl("source.")

# COMMAND ----------

ce = spark.sql(f'''
    SELECT
        d.cost_element,
        d.chart_of_accounts,
        d.cost_element_description_long,
        MAX(CASE
            WHEN h.hierarchy_id = 'OP01_CE'
                THEN h.level_2_node
        END) AS primary_cost,

        MAX(CASE
            WHEN h.hierarchy_id = 'OP01_CE'
                THEN h.level_2_node_text
        END) AS primary_cost_description,

        MAX(CASE
            WHEN h.hierarchy_id = 'OP01_CEG'
                THEN h.level_3_node
        END) AS total_opex,

        MAX(CASE
            WHEN h.hierarchy_id = 'OP01_CEG'
                THEN h.level_3_node_text
        END) AS total_opex_description,

        d.__etl_keys_fprint,
        xxhash64(
            d.cost_element,
            d.cost_element_description_long,
            MAX(CASE
                WHEN h.hierarchy_id = 'OP01_CE'
                    THEN h.level_2_node
            END),
            MAX(CASE
                WHEN h.hierarchy_id = 'OP01_CE'
                    THEN h.level_2_node_text
            END),
            MAX(CASE
                WHEN h.hierarchy_id = 'OP01_CEG'
                    THEN h.level_3_node
            END),
            MAX(CASE
                WHEN h.hierarchy_id = 'OP01_CEG'
                    THEN h.level_3_node_text
            END)
        ) AS __etl_row_fprint,
        d.__etl_effective_from,
        d.__etl_effective_to,
        d.__etl_is_active,
        d.__etl_is_deleted
    FROM {casted_tablename} AS d
    INNER JOIN {env_vars.silver_catalog}.fin_controlling.cost_element_hierarchy AS h 
        ON d.cost_element = h.cost_element_hierarchy_node
    WHERE chart_of_accounts = 'OP01'
    GROUP BY
        d.cost_element,
        d.chart_of_accounts,
        d.cost_element_description_long,
        d.__etl_keys_fprint,
        d.__etl_effective_from,
        d.__etl_effective_to,
        d.__etl_is_active,
        d.__etl_is_deleted;
''').createOrReplaceTempView('ce')

# COMMAND ----------

select_ce = spark.sql(f'''
    SELECT * FROM ce
        WHERE ce.primary_cost IS NOT NULL 
            AND 
        ce.total_opex IS NOT NULL                 
''').createOrReplaceTempView('select_ce')

# COMMAND ----------

merge_result = spark.sql(f'''    
    MERGE INTO {dest_tablename} AS target
    USING select_ce AS source
        ON target.__etl_keys_fprint = source.__etl_keys_fprint
    WHEN MATCHED THEN
        UPDATE SET 
            target.cost_element = source.cost_element,
            target.chart_of_accounts = source.chart_of_accounts,
            target.cost_element_description_long = source.cost_element_description_long,
            target.primary_cost = source.primary_cost,
            target.primary_cost_description = source.primary_cost_description,
            target.total_opex = source.total_opex,
            target.total_opex_description = source.total_opex_description,
            target.__etl_row_fprint = source.__etl_row_fprint,
            target.__etl_effective_from = source.__etl_effective_from,
            target.__etl_effective_to = source.__etl_effective_to,
            target.__etl_is_active = source.__etl_is_active,
            target.__etl_is_deleted = source.__etl_is_deleted
    WHEN NOT MATCHED THEN
        INSERT (
            cost_element,
            chart_of_accounts,
            cost_element_description_long,
            primary_cost,
            primary_cost_description,
            total_opex,
            total_opex_description,
            __etl_keys_fprint,
            __etl_row_fprint,
            __etl_effective_from,
            __etl_effective_to,
            __etl_is_active,
            __etl_is_deleted
        ) VALUES (
            source.cost_element,
            source.chart_of_accounts,
            source.cost_element_description_long,
            source.primary_cost,
            source.primary_cost_description,
            source.total_opex,
            source.total_opex_description,
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