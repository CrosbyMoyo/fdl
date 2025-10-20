# Databricks notebook source
# MAGIC %md
# MAGIC ## SGR Adjustments Bronze to Silver
# MAGIC
# MAGIC Generic Notebook moves the `{bronze}.ftp_vbox.sgr_manual_adjustments` data to a staging table defined in the metadata.
# MAGIC
# MAGIC This notebook further refines that data, and adds the payload fingerprint.  Then merges the data into `{silver}.fin_general_ledger.sgr_manual_adjustments` table.

# COMMAND ----------

# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

# change the yaml file destination
metadata_filename = "silver.sgr_manual_adjustments.yaml"
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

sgr = spark.sql(f'''
    SELECT 
        src.fiscal_year,
        src.dimension,
        src.ledger,
        src.subitem_category,
        src.subitem,
        src.posting_level,
        src.document_type,
        src.amount_in_local_currency,
        src.date_created,
        src.chart_of_accounts,
        src.quantity,
        src.gl_account,
        src.base_unit_of_measure,
        src.material_number,
        src.fiscal_year_period,
        src.consolidation_chart_of_accounts,
        src.cost_center,
        src.profit_center,
        src.controlling_area,
        src.record_type,
        src.consolidation_version,
        src.local_currency,
        src.posting_period,
        src.document_category,
        src.consolidation_unit,
        src.financial_statement_item,
        src.__etl_keys_fprint,
        xxhash64({row_fprint_ddl}) AS __etl_row_fprint,
        src.__etl_effective_from,
        src.__etl_effective_to,
        src.__etl_is_active,
        src.__etl_is_deleted
    FROM {casted_tablename} AS src;     
''').createOrReplaceTempView('sgr')

# COMMAND ----------

merge_result = spark.sql(f'''    
    INSERT OVERWRITE {dest_tablename}
    SELECT 
        src.fiscal_year,
        src.dimension,
        src.ledger,
        src.subitem_category,
        src.subitem,
        src.posting_level,
        src.document_type,
        src.amount_in_local_currency,
        src.date_created,
        src.chart_of_accounts,
        src.quantity,
        src.gl_account,
        src.base_unit_of_measure,
        src.material_number,
        src.fiscal_year_period,
        src.consolidation_chart_of_accounts,
        src.cost_center,
        src.profit_center,
        src.controlling_area,
        src.record_type,
        src.consolidation_version,
        src.local_currency,
        src.posting_period,
        src.document_category,
        src.consolidation_unit,
        src.financial_statement_item,
        src.__etl_keys_fprint,
        xxhash64({row_fprint_ddl}) AS __etl_row_fprint,
        src.__etl_effective_from,
        src.__etl_effective_to,
        src.__etl_is_active,
        src.__etl_is_deleted
    FROM sgr AS src;
''')

# COMMAND ----------

logger.log.info(f'Merge: {dest_tablename} {merge_result.toPandas().head(1)}')