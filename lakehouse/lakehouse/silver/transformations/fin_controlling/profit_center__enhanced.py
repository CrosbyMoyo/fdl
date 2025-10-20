# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.profit_center.yaml"
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

casted_tablename = f'{env_vars.silver_catalog}.fin_controlling_staging.profit_center__casted'
flattened_tablename = f'{env_vars.silver_catalog}.fin_controlling_staging.profit_center_flat_hierarchy'
dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", True)}'

# COMMAND ----------

segments = spark.sql(
    f'''
        SELECT
            seg.node_id      AS profit_center
            ,seg.level2_node AS segment
            ,seg.level2_text AS segment_description
            ,seg.level3_node AS line_of_business 
            ,seg.level3_text AS line_of_business_description
            ,seg.level5_node AS line_of_business_1
            ,seg.level5_text AS line_of_business_1_description
            ,IF (
                seg.level2_node LIKE '%SEG_RETAIL%'
                OR seg.level2_node LIKE '%SEG_COMM%'
                OR seg.level2_node LIKE '%SEG_LUB%'
                OR seg.level2_node LIKE '%SEG_SUPPLY%'
                , True
                , False 
            ) AS volume_flag_ind
        FROM 
            {flattened_tablename} AS seg
        WHERE 
            seg.hier_id = "SEG_TOTAL" 
            AND seg.leaf_flag = TRUE
    '''
)
segments.createOrReplaceTempView('segments')

# COMMAND ----------

zrtr_cepc_joined = spark.sql(
    f'''
        SELECT 
            c.profit_center,
            t.profit_center_description,
            c.controlling_area,
            {env_vars.silver_catalog}.ca_cross_application_components.extract_sap_date(c.valid_from) AS valid_from,
            {env_vars.silver_catalog}.ca_cross_application_components.extract_sap_date(c.valid_to) AS valid_to,
            sub.sales_organization,
            sub.sales_organization_description,
            sub.distribution_channel,
            sub.distribution_channel_description,
            sub.division,
            sub.sales_division_description AS division_description
        FROM {casted_tablename} AS c
        LEFT JOIN {env_vars.silver_catalog}.fin_controlling.profit_center_substitution AS sub
            ON c.profit_center = sub.profit_center
        LEFT JOIN {env_vars.silver_catalog}.fin_controlling.profit_center_master_data_texts AS t
            ON c.profit_center = t.profit_center
        QUALIFY 
            row_number() OVER (
                PARTITION BY c.profit_center
                ORDER BY 
                    sub.sales_organization ASC,
                    sub.distribution_channel ASC,
                    sub.division ASC
                ) = 1
    '''
) 
zrtr_cepc_joined.createOrReplaceTempView('zrtr_cepc_joined')

# COMMAND ----------

enhanced = spark.sql(
    f'''
        SELECT 
            s.*
            ,coalesce(p.profit_center_description, '')        AS profit_center_description
            ,coalesce(p.controlling_area, 'OP01')             AS controlling_area
            ,coalesce(p.valid_from, '1900-01-01')             AS valid_from 
            ,coalesce(p.valid_to, '9999-12-31')               AS valid_to
            ,coalesce(p.sales_organization, '')               AS sales_organization
            ,coalesce(p.sales_organization_description, '')   AS sales_organization_description
            ,coalesce(p.distribution_channel, '')             AS distribution_channel
            ,coalesce(p.distribution_channel_description, '') AS distribution_channel_description
            ,coalesce(p.division, '')                         AS division
            ,coalesce(p.division_description, '')             AS division_description
        FROM 
            segments AS s 
        LEFT JOIN zrtr_cepc_joined AS p
            ON s.profit_center = p.profit_center
    '''
)
enhanced.createOrReplaceTempView('enhanced')

# COMMAND ----------

spark.sql(
    f'''
        SELECT 
            {metadata.get_key_columns_ddl()}
            ,{metadata.get_payload_columns_ddl()}
            ,{metadata.get_key_fprint_ddl()}                AS __etl_keys_fprint
            ,{metadata.get_row_fprint_ddl()}                AS __etl_row_fprint
            ,current_date()                                 AS __etl_effective_from
            ,CAST(NULL AS DATE)                             AS __etl_effective_to
            ,True                                           AS __etl_is_active
            ,False                                          AS __etl_is_deleted
        FROM
            enhanced
    '''
).createOrReplaceTempView('final')

# COMMAND ----------

merge_statement = metadata.get_merge_ddl('final', dest_tablename)
merge_result = spark.sql(merge_statement)
logger.log.info(f'Merge: {dest_tablename} {merge_result.toPandas().head(1)}')