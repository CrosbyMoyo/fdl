# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

dbutils.widgets.text(
    name='metadata_filename',
    defaultValue='gold.dim.vcodes.yaml',
    label='1 - metadata_filename'
)

# COMMAND ----------

metadata_filename = dbutils.widgets.get('metadata_filename')
logger.log.info(f'Widget: metadata_filename = "{metadata_filename}"')

# COMMAND ----------

metadata = GoldMetadataYaml(
    file_path=f'./metadata/{metadata_filename}',
    slv_catalog=env_vars.silver_catalog,
    gld_catalog=env_vars.gold_catalog
)


# COMMAND ----------

vcodes_tablename = metadata.source_3partname(
    tablename='vcodes',
    include_schemaversion=True
)

dest_tablename = metadata.dest_3partname(
    include_schemaversion=True
)

# COMMAND ----------

gold_table_query = f'''
    SELECT 
        vc.vcode,
        vc.description,
        vc.c1_relevant_flag,
        vc.c2_relevant_flag,
        vc.c3_relevant_flag,
        vc.c4_relevant_flag,
        vc.net_income_relevant_flag,
        vc.local_ebitda_relevant_flag,
        vc.local_opex,
        CASE 
            WHEN vc.local_opex = "C" THEN "Central Opex"
            WHEN vc.local_opex = "L" THEN "Local Opex"
            ELSE NULL
        END AS opex_description,
        vc.opex_type,
        CASE 
            WHEN vc.opex_type = "V" THEN "Variable Opex"
            WHEN vc.opex_type = "F" THEN "Fixed Opex"
            ELSE NULL
        END AS opex_type_description,
        vc.direct_contribution_relevant_flag,
        vc.indirect_contribution_relevant_flag,
        vc.vcode_sort_order
    FROM 
        {vcodes_tablename} AS vc
'''

# COMMAND ----------

write_result = metadata.process_dim_transformation_query(gold_table_query)
logger.log.info(f'Write: {dest_tablename} {write_result}')