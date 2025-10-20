# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = "silver.profit_loss_subcategory.yaml"
logger.log.info(f'"metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

source_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("vcodes_hierarchy", include_schemaversion=True)}'

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

gl = spark.sql(f'''
        SELECT DISTINCT
            v.vcode_hierarchy_node AS vcode,
            v.vcode_description AS sub_category,
            CASE
                WHEN v.vcode_hierarchy_node IN ('P.211.1', 'P.212.1') 
                    THEN 'TPTE'
                WHEN v.vcode_hierarchy_node = 'P.34.1' 
                    THEN 'P.34.1'
                WHEN v.vcode_hierarchy_node = 'P.35.1' 
                    THEN 'P.35.1'
                WHEN v.vcode_hierarchy_node IN ('P.36','P.44B','P.43B','P.36.1','P.433.1','P.44.4','P.36.2','P.433.2','P.44.3') 
                    THEN 'N13'
                ELSE
                    CASE
                        WHEN v.hierarchy_level = '2' 
                            THEN level_1_node
                        WHEN v.hierarchy_level = '3' 
                            THEN level_2_node
                        WHEN v.hierarchy_level = '4' 
                            THEN level_3_node
                        WHEN v.hierarchy_level = '5' 
                            THEN level_4_node
                        WHEN v.hierarchy_level = '6' 
                            THEN level_5_node
                        WHEN v.hierarchy_level = '7' 
                            THEN level_6_node
                        WHEN v.hierarchy_level = '8' 
                            THEN level_7_node
                        WHEN v.hierarchy_level = '9' 
                            THEN level_8_node
                        WHEN v.hierarchy_level = '10' 
                            THEN level_9_node
                        WHEN v.hierarchy_level = '11' 
                            THEN level_10_node
                        WHEN v.hierarchy_level = '12' 
                            THEN level_11_node
                        ELSE 'CHECK'
                    END
            END AS Parent,

            CASE
                WHEN v.vcode_hierarchy_node IN ('P.211.1', 'P.212.1') 
                    THEN 'Tot Prim. Tpt Expenses'
                WHEN v.vcode_hierarchy_node = 'P.34.1' 
                    THEN 'Supply/Processing Expenses'
                WHEN v.vcode_hierarchy_node = 'P.35.1' 
                    THEN 'MK Processing Plants'
                WHEN v.vcode_hierarchy_node IN ('P.36','P.44B','P.43B','P.36.1','P.433.1','P.44.4','P.36.2','P.433.2','P.44.3') 
                    THEN 'Depreciation and Amortisation'
                ELSE
                    CASE
                        WHEN v.hierarchy_level = '2' 
                            THEN level_1_node_text
                        WHEN v.hierarchy_level = '3' 
                            THEN level_2_node_text
                        WHEN v.hierarchy_level = '4' 
                            THEN level_3_node_text
                        WHEN v.hierarchy_level = '5' 
                            THEN level_4_node_text
                        WHEN v.hierarchy_level = '6' 
                            THEN level_5_node_text
                        WHEN v.hierarchy_level = '7' 
                            THEN level_6_node_text
                        WHEN v.hierarchy_level = '8' 
                            THEN level_7_node_text
                        WHEN v.hierarchy_level = '9' 
                            THEN level_8_node_text
                        WHEN v.hierarchy_level = '10' 
                            THEN level_9_node_text
                        WHEN v.hierarchy_level = '11' 
                            THEN level_10_node_text
                        WHEN v.hierarchy_level = '12' 
                            THEN level_11_node_text
                        ELSE 'CHECK'
                    END
            END AS parent_category,

            CASE
                WHEN v.hierarchy_level = '2' 
                    THEN level_1_node
                WHEN v.hierarchy_level = '3' 
                    THEN level_1_node
                WHEN v.hierarchy_level = '4' 
                    THEN level_2_node
                WHEN v.hierarchy_level = '5' 
                    THEN level_3_node
                WHEN v.hierarchy_level = '6' 
                    THEN level_4_node
                WHEN v.hierarchy_level = '7' 
                    THEN level_5_node
                WHEN v.hierarchy_level = '8' 
                    THEN level_6_node
                WHEN v.hierarchy_level = '9' 
                    THEN level_7_node
                WHEN v.hierarchy_level = '10' 
                    THEN level_8_node
                WHEN v.hierarchy_level = '11' 
                    THEN level_9_node
                WHEN v.hierarchy_level = '12' 
                    THEN level_10_node
                ELSE 'CHECK'
            END AS master_parent,

            CASE
                WHEN v.hierarchy_level = '2' 
                    THEN level_1_node_text
                WHEN v.hierarchy_level = '3' 
                    THEN level_1_node_text
                WHEN v.hierarchy_level = '4' 
                    THEN level_2_node_text
                WHEN v.hierarchy_level = '5' 
                    THEN level_3_node_text
                WHEN v.hierarchy_level = '6' 
                    THEN level_4_node_text
                WHEN v.hierarchy_level = '7' 
                    THEN level_5_node_text
                WHEN v.hierarchy_level = '8' 
                    THEN level_6_node_text
                WHEN v.hierarchy_level = '9' 
                    THEN level_7_node_text
                WHEN v.hierarchy_level = '10' 
                    THEN level_8_node_text
                WHEN v.hierarchy_level = '11' 
                    THEN level_9_node_text
                WHEN v.hierarchy_level = '12' 
                    THEN level_10_node_text
                ELSE 'CHECK'
            END AS master_parent_category
        FROM
            {source_tablename} AS v 
        WHERE
            v.hierarchy_id = 'MIP'
            AND (
                v.vcode_hierarchy_node like 'P%'
                or v.vcode_hierarchy_node like 'V%'
            )
            AND v.is_leaf_node = true

    UNION ALL

        SELECT
            *
        FROM
            VALUES
                ('v.001', 'Volume', 'v.001', 'Volume', 'v.001', 'Volume')
            AS (
                vcode,
                sub_category,
                Parent,
                parent_category,
                master_parent,
                master_parent_category
            )
''').createOrReplaceTempView("plsubcat")

# COMMAND ----------

write_result = metadata.process_transformation_table('plsubcat', env_vars.silver_catalog)
logger.log.info(f'Write: {dest_tablename} {write_result}')