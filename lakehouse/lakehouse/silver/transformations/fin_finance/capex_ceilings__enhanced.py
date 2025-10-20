# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')


# COMMAND ----------

metadata_filename = "silver.capex_ceilings.yaml"
logger.log.info(f'"metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", include_schemaversion=True)}'

# COMMAND ----------

#TODO: This needs to live in a seed file and built as a lookup / ref table 
lob_ceiling_lookup = spark.sql(
    f'''
          SELECT * FROM VALUES 
            ('1', 'LOB511'), 
            ('2', 'LOB211'),
            ('3', 'LOB221'),
            ('4', 'LOB311'),
            ('5', 'LOB411'),
            ('6', 'Bitumen'),
            ('7', 'Export Lubes'),
            ('9', 'LOB_CORP2'),
            ('10', 'LOB131'),
            ('12', 'LOB_MSD2'),
            ('13', 'LOB52'),
            ('14', 'Aviation Lubes'),
            ('16', 'LOB62'),
            ('17', 'LOB631'),
            ('18', 'Marine Lubes'),
            ('19', 'LOB412'),
            ('20', 'LOB64'),
            ('21', 'LOB111'),
            ('22', 'LOB121'),
            ('23', 'LOB151'),
            ('24', 'LOB131'),
            ('25', 'LOB251')
        AS t(lob_ceiling_id, lob_ceiling_name)
    '''
)

lob_ceiling_lookup.createOrReplaceTempView('lob_ceiling_lookup')

# COMMAND ----------

spark.sql(
    f'''
        WITH countries AS 
        (
            SELECT
            c.country_id
            ,c.country_code
            ,c.country_name
            ,CASE 
                WHEN c.country_code = 'CEN' THEN 'NLH2' ELSE concat(c.country_code,'01') END AS company_code
        FROM 
            {env_vars.silver_catalog}.fin_finance.capex_countries AS c
        ),
        lob AS(
            SELECT 
                l.lob_id
                ,l.line_of_business_name
            FROM 
                {env_vars.silver_catalog}.fin_finance.lob AS l
        ),
        modelsubtype AS(
            SELECT 
                ms.model_subtype_id
                ,ms.subtype_name
            FROM
                {env_vars.silver_catalog}.fin_finance.modelsubtype AS ms
        ),
        country_lob_budget AS(
            SELECT
                cb.country_lob_budget_id
                ,cb.line_of_business_id
                ,cb.model_subtype_id
                ,c.country_code
                ,cb.budget_year
                ,cb.budget 
                ,c.company_code
            FROM 
                {env_vars.silver_catalog}.fin_finance.country_lob_budget AS cb
            LEFT JOIN 
                countries AS c ON c.country_id = cb.country_id
        ),
        result1 AS(
            SELECT 
                cllb.country_lob_budget_id
                ,cllb.line_of_business_id
                ,cllb.model_subtype_id
                ,cllb.country_code
                ,l.line_of_business_name
                ,cllb.budget_year
                ,cllb.budget
                ,cllb.company_code
            FROM
                country_lob_budget AS cllb
            LEFT JOIN 
                lob AS l ON l.lob_id = cllb.line_of_business_id
        ), 
        result2 AS(
            SELECT
                r.country_lob_budget_id
                ,ms.subtype_name
                ,r.line_of_business_id
                ,r.model_subtype_id
                ,r.country_code
                ,r.line_of_business_name
                ,r.budget_year
                ,r.budget
                ,r.company_code
            FROM 
                result1 AS r
            LEFT JOIN 
                modelsubtype AS ms 
                    ON ms.model_subtype_id = r.model_subtype_id
        )
        SELECT 
            r2.country_lob_budget_id AS ceiling_id
            ,r2.budget_year
            ,CASE WHEN r2.country_code = 'CEN' THEN '' ELSE r2.country_code END AS country_code
            ,r2.company_code
            ,r2.line_of_business_name
            ,cc.currency_key
            ,lc.lob_ceiling_name AS lob1_ceiling
            ,r2.subtype_name
            ,r2.budget AS ceiling
        FROM 
            result2 AS r2
        JOIN 
            {env_vars.silver_catalog}.ca_cross_application_components.company_code AS cc
                ON cc.company_code = r2.company_code
        LEFT JOIN
            lob_ceiling_lookup AS lc
                ON lc.lob_ceiling_id = r2.line_of_business_id
        WHERE 
            r2.budget != 0
    '''
).createOrReplaceTempView('ceilings')

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
            ceilings
    '''
).createOrReplaceTempView('capex_ceilings')

# COMMAND ----------

merge_statement = metadata.get_merge_ddl('capex_ceilings', dest_tablename)
merge_result = spark.sql(merge_statement)
logger.log.info(f'Merge: {dest_tablename} {merge_result.toPandas().head(1)}')