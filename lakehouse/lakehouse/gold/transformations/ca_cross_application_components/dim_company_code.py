# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

dbutils.widgets.text(
    name='metadata_filename',
    defaultValue='gold.dim.company_code.yaml',
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

cc_tablename = metadata.source_3partname(
    tablename='company_code',
    include_schemaversion=True
)

dest_tablename = metadata.dest_3partname(
    include_schemaversion=True
)

# COMMAND ----------

gold_table_query = f'''
        SELECT
            cc.company_code
            ,cc.company_name
            ,cc.country_key 
            ,cc.city
            ,cc.currency_key AS currency_skey
            ,cc.chart_of_accounts
            ,cc.credit_control_area
            ,cc.display_name
            ,cc.reporting_entity
            ,cc.geo_region
            ,cc.vivo_group
            ,cc.entity_grouping_level_top
            ,cc.entity_grouping_level_0
            ,cc.entity_grouping_level_1
            ,cc.operating_unit
            ,cc.entity_grouping_level_2_geographical
            ,cc.entity_grouping_level_3_vp_reporting
            ,cc.region_alternative_2
            ,cc.planning_company_code
            ,cc.central_credit_country_grouping
            ,cc.reporting_entity_ri
        FROM
            {cc_tablename} AS cc
'''

# COMMAND ----------

write_result = metadata.process_dim_transformation_query(gold_table_query)
logger.log.info(f'Write: {dest_tablename} {write_result}')