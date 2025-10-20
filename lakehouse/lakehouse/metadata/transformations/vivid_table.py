# Databricks notebook source
# MAGIC %run ../../common/properties

# COMMAND ----------

spark.sql(f'''
    MERGE INTO vivid_meta.vivid_meta.vivid_table AS target
        USING {env_vars.bronze_catalog}.sap_s4hana.dd02t AS source
            ON UPPER(target.source_table_name) = source.TABNAME
            AND source.DDLANGUAGE = 'E'

        WHEN MATCHED AND target.source_table_description IS NULL AND target.source_system = "sap_s4hana"
        THEN UPDATE SET target.source_table_description = source.DDTEXT;
''')

# COMMAND ----------

spark.sql('''
    MERGE INTO vivid_meta.vivid_meta.vivid_table AS target
        USING (
            SELECT 
                *,
                -- transform description by removing special characters and replacing spaces with underscores
            vivid_meta.vivid_meta.get_derived_name(source_table_description) AS derived_table_name
            FROM vivid_meta.vivid_meta.vivid_table
        ) AS source
        ON target.source_table_name = source.source_table_name
        WHEN MATCHED AND target.vivid_derived_table_name IS NULL AND source.source_system = "sap_s4hana"
        THEN UPDATE SET target.vivid_derived_table_name = source.derived_table_name;        
''')