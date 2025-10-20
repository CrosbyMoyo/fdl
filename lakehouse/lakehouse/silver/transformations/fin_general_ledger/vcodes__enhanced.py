# Databricks notebook source
# MAGIC %md
# MAGIC ## 1pc_vcodes Bronze to Silver
# MAGIC
# MAGIC Generic Notebook moves the `{bronze}.ftp_vbox.1pc_vcodes` data to a staging table defined in the metadata.
# MAGIC
# MAGIC This notebook further refines that data, and adds the payload fingerprint.  Then merges the data into `{silver}.fin_general_ledger.vcodes` table.

# COMMAND ----------

# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.1pc_vcodes.yaml"
logger.log.info(f'"metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

source_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("casted", include_schemaversion=True)}'

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", include_schemaversion=True)}'

# COMMAND ----------

# get the columns to xxhash64
row_fprint_ddl = metadata.get_payload_columns_ddl("am_enhanced.")
# get the columns to MATCH
match_cols_ddl = metadata.get_update_set_ddl("source.", "target.")
# get the target columns to INSERT
insert_cols_tgt_ddl = metadata.get_insert_ddl()
# get the source columns to INSERT
insert_cols_src_ddl = metadata.get_insert_ddl("source.")

# COMMAND ----------

am_enhanced = spark.sql(f'''
    SELECT DISTINCT
        -- key
        source.vcode,

        --payload
        source.description,
        {env_vars.silver_catalog}.ca_cross_application_components.cast_sap_indicator(source.indirect_contribution_relevant_flag) AS indirect_contribution_relevant_flag,
        {env_vars.silver_catalog}.ca_cross_application_components.cast_sap_indicator(source.net_income_relevant_flag) AS net_income_relevant_flag,
        {env_vars.silver_catalog}.ca_cross_application_components.cast_sap_indicator(source.c1_relevant_flag) AS c1_relevant_flag,
        {env_vars.silver_catalog}.ca_cross_application_components.cast_sap_indicator(source.c2_relevant_flag) AS c2_relevant_flag,
        {env_vars.silver_catalog}.ca_cross_application_components.cast_sap_indicator(source.c3_relevant_flag) AS c3_relevant_flag,
        {env_vars.silver_catalog}.ca_cross_application_components.cast_sap_indicator(source.c4_relevant_flag) AS c4_relevant_flag,
        source.local_opex,
        source.opex_type,
        {env_vars.silver_catalog}.ca_cross_application_components.cast_sap_indicator(source.local_ebitda_relevant_flag) AS local_ebitda_relevant_flag,
        {env_vars.silver_catalog}.ca_cross_application_components.cast_sap_indicator(source.direct_contribution_relevant_flag) AS direct_contribution_relevant_flag,
        CAST(LEFT(source.vcode_sort_order, CHARINDEX('-', source.vcode_sort_order) - 1) AS INT) AS vcode_sort_order,

        --metadata
        source.__etl_keys_fprint,
        source.__etl_effective_from,
        source.__etl_effective_to,
        source.__etl_is_active,
        source.__etl_is_deleted
    FROM
        {source_tablename} AS source
    INNER JOIN {env_vars.silver_catalog}.fin_general_ledger.vcodes_hierarchy AS hier
        ON source.vcode = hier.vcode_hierarchy_node
    WHERE hier.is_leaf_node = TRUE;                   
''').createOrReplaceTempView('am_enhanced')

# COMMAND ----------

am_hashed = spark.sql(f'''
    SELECT
        *
        ,xxhash64({row_fprint_ddl}) AS __etl_row_fprint
    FROM
        am_enhanced;
''').createOrReplaceTempView('am_hashed')

# COMMAND ----------

merge_result = spark.sql(f'''

    WITH am_enhanced AS (
        SELECT DISTINCT
            -- key
            source.vcode,

            --payload
            source.description,
            {env_vars.silver_catalog}.ca_cross_application_components.cast_sap_indicator(source.indirect_contribution_relevant_flag) AS indirect_contribution_relevant_flag,
            {env_vars.silver_catalog}.ca_cross_application_components.cast_sap_indicator(source.net_income_relevant_flag) AS net_income_relevant_flag,
            {env_vars.silver_catalog}.ca_cross_application_components.cast_sap_indicator(source.c1_relevant_flag) AS c1_relevant_flag,
            {env_vars.silver_catalog}.ca_cross_application_components.cast_sap_indicator(source.c2_relevant_flag) AS c2_relevant_flag,
            {env_vars.silver_catalog}.ca_cross_application_components.cast_sap_indicator(source.c3_relevant_flag) AS c3_relevant_flag,
            {env_vars.silver_catalog}.ca_cross_application_components.cast_sap_indicator(source.c4_relevant_flag) AS c4_relevant_flag,
            source.local_opex,
            source.opex_type,
            {env_vars.silver_catalog}.ca_cross_application_components.cast_sap_indicator(source.local_ebitda_relevant_flag) AS local_ebitda_relevant_flag,
            {env_vars.silver_catalog}.ca_cross_application_components.cast_sap_indicator(source.direct_contribution_relevant_flag) AS direct_contribution_relevant_flag,
            CAST(LEFT(source.vcode_sort_order, CHARINDEX('-', source.vcode_sort_order) - 1) AS INT) AS vcode_sort_order,

            --metadata
            source.__etl_keys_fprint,
            source.__etl_effective_from,
            source.__etl_effective_to,
            source.__etl_is_active,
            source.__etl_is_deleted
        FROM
            {source_tablename} AS source
    ),
    am_hashed AS (
        SELECT
            *
            ,xxhash64({row_fprint_ddl}) AS __etl_row_fprint
        FROM
            am_enhanced
    )
    MERGE INTO {dest_tablename} AS target
    USING am_hashed AS source
        ON source.__etl_keys_fprint = target.__etl_keys_fprint
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
        )
        VALUES (
            {insert_cols_src_ddl}
        );
''')

# COMMAND ----------

logger.log.info(f'Merge: {dest_tablename} {merge_result.toPandas().head(1)}')