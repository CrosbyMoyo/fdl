# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.profit_loss_category.yaml"
logger.log.info(f'"metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

source_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("vcodes_hierarchy", include_schemaversion=True)}'

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", include_schemaversion=True)}'

# COMMAND ----------

source_tablename

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

gl = spark.sql(f'''
        SELECT DISTINCT 
            CASE 
                WHEN v.level_10_node = 'P.11' THEN 1 
                WHEN v.level_10_node = 'P.12' THEN 2
                WHEN v.level_10_node = 'P.323.1' THEN 4
                ELSE 9999
            END AS  rank
            ,CASE 
                WHEN v.level_10_node = 'P.323.1' THEN 'N11'
                ELSE v.level_10_node
            END AS parent_id
            ,v.level_10_node_text AS  pl_category
            FROM
                {source_tablename} AS v
            WHERE 
                v.hierarchy_id = 'MIP'
                AND (v.vcode_hierarchy_node LIKE 'P%' OR v.vcode_hierarchy_node LIKE 'V%')
                AND v.is_leaf_node = True 
                AND v.level_10_node IN ('P.11', 'P.12', 'P.323.1')

    UNION ALL

        SELECT
            *
        FROM
            VALUES 
                 (0, 'v.001', 'Volume')
                ,(3, 'TPTE', 'Tot Prim. Tpt Expenses')
                ,(5, 'N12', 'Secondary Transport')
                ,(6, 'P.34.1', 'Supply/Processing Expenses')
                ,(7, 'P.35.1', 'MK Processing Plants')
                ,(8, 'P.42', 'Other Income/Expenses')
                ,(9, 'P.45', 'Share of profit of Assoc/JV')
                ,(10, 'D15', 'Selling and marketing cost')
                ,(11, 'D22', 'Local Overheads (G&A)')
                ,(12, 'P.52', 'Central Support Costs')
                ,(13, 'D16', 'Total Brand Fees')
                ,(14, 'N13', 'Depreciation and Amortisation')
                ,(15, 'P.72', 'Finance Expense')
                ,(16, 'P.8', 'Income Taxes')
            AS (
                rank
                ,parent_id
                ,pl_category
            )
''').createOrReplaceTempView("plcat")

# COMMAND ----------

write_result = metadata.process_transformation_table('plcat', env_vars.silver_catalog)
logger.log.info(f'Write: {dest_tablename} {write_result}')