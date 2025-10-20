# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = './metadata/silver.company_code.yaml'
logger.log.info(f'Widget: metadata_filename = "{metadata_filename}"')

# COMMAND ----------

metadata = MetadataYaml(metadata_filename)


# COMMAND ----------

t001_source = f'{env_vars.silver_catalog}.ca_cross_application_components_staging.company_codes__casted'
flat_compcode_source = f'{env_vars.silver_catalog}.ca_cross_application_components.flat_compcode'
dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", True)}'

# COMMAND ----------

flat_compcode = spark.sql(
    f'''
        SELECT 
            pc.*
            ,CASE 
                WHEN 
                    pc.vivo_group IN (
                        "East"
                        ,"South"
                        ,"West"
                        ,"Maghreb & Indian Ocean"
                    ) 
                    THEN LEFT("reporting_entity", 50) 
                ELSE "Non-Operating"
            END AS operating_unit
            ,CASE
                WHEN 
                    pc.vivo_group IN (
                        "East & South", 
                        "East",
                        "South",
                        "West",
                        "Maghreb & Indian Ocean"
                    )
                    THEN pc.vivo_group
                ELSE "Non-Operating"
            END AS region_alternative_2
        FROM 
            {flat_compcode_source} AS pc
        WHERE
            pc.company_code <> ''
    '''
)

flat_compcode.createOrReplaceTempView('flat_compcode')

# COMMAND ----------

# Take everything in the t001 and get the additional info from the flat file 
t001 = spark.sql(
    f'''
        SELECT 
            c.company_code
            ,c.company_name
            ,c.chart_of_accounts
            ,c.country_key
            ,c.city
            ,c.currency_key
            ,c.credit_control_area
            ,c.preferred_language
            ,coalesce(pc.display_name, '') AS display_name
            ,coalesce(pc.reporting_entity, '') AS reporting_entity
            ,coalesce(pc.geo_region, '') AS geo_region
            ,coalesce(pc.vivo_group, '') AS vivo_group
            ,coalesce(pc.entity_grouping_level_0, '') AS entity_grouping_level_0
            ,coalesce(pc.entity_grouping_level_1, '') AS entity_grouping_level_1
            ,coalesce(pc.entity_grouping_level_top, '') AS entity_grouping_level_top
            ,coalesce(pc.entity_grouping_level_2_geographical, '') AS entity_grouping_level_2_geographical
            ,coalesce(pc.entity_grouping_level_3_vp_reporting, '') AS entity_grouping_level_3_vp_reporting
            ,coalesce(pc.planning_company_code, '') AS planning_company_code
            ,coalesce(pc.central_credit_country_grouping, '') AS central_credit_country_grouping
            ,coalesce(pc.reporting_entity_ri, '') AS reporting_entity_ri
            ,coalesce(pc.operating_unit, '') AS operating_unit
            ,coalesce(pc.region_alternative_2) AS region_alternative_2
        FROM 
            {t001_source} AS c
        LEFT JOIN flat_compcode AS pc
            ON c.company_code = pc.company_code
    '''
)

t001.createOrReplaceTempView('t001')

# COMMAND ----------

remaining_flat_compcode = spark.sql(
    f'''
        -- Populate with defaults for company entities
        SELECT 
            pc.company_code 
            ,pc.display_name AS company_name
            ,'OP01' AS chart_of_accounts
            ,CASE 
                WHEN pc.company_code = 'ZWH2' THEN 'ZW'
                ELSE 'NL'  
             END AS country_key
            ,'Amsterdam' AS city
            ,pc.currency AS currency_key
            ,'NL00' AS credit_control_area
            ,'E' AS preferred_language
            ,pc.display_name
            ,pc.reporting_entity
            ,pc.geo_region
            ,pc.vivo_group
            ,pc.entity_grouping_level_0
            ,pc.entity_grouping_level_1
            ,pc.entity_grouping_level_top
            ,pc.entity_grouping_level_2_geographical
            ,pc.entity_grouping_level_3_vp_reporting
            ,pc.planning_company_code
            ,pc.central_credit_country_grouping
            ,pc.reporting_entity_ri
            ,pc.operating_unit
            ,pc.region_alternative_2

        FROM 
            flat_compcode AS pc 
        ANTI JOIN t001 AS c 
            ON c.company_code = pc.company_code
    '''
)

remaining_flat_compcode.createOrReplaceTempView('remaining_flat_compcode')

# COMMAND ----------

enhanced = spark.sql(
    f'''
            SELECT t.* FROM t001 AS t 
        UNION ALL 
            SELECT f.* FROM remaining_flat_compcode AS f 
    '''
)

enhanced.createOrReplaceTempView('enhanced')

# COMMAND ----------

spark.sql(
    f'''
        SELECT 
            {metadata.get_key_columns_ddl()}
            ,{metadata.get_payload_columns_ddl()}
            ,xxhash64({metadata.get_key_columns_ddl()})     AS __etl_keys_fprint
            ,xxhash64({metadata.get_payload_columns_ddl()}) AS __etl_row_fprint
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