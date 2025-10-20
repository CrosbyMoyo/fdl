# Databricks notebook source
# MAGIC %run ../../common/properties

# COMMAND ----------

spark.sql(f'''
    -- join dd03l, dd04t and vivid_datatype_mapping tables to populate the vivid_field table
    WITH vivid_sap_s4_fields AS (
        SELECT
            vt.source_system, 
            vt.source_table_name,
            dd03.FIELDNAME AS source_field_name,
            -- transfrom the sap indicators to booleans
            vivid_meta.vivid_meta.convert_sap_indicator_to_boolean(dd03.KEYFLAG) AS source_field_primary_key_flag,
            dd03.DATATYPE AS source_data_type,
            CAST(dd03.LENG AS INT) AS source_field_length,
            CAST(dd03.DECIMALS AS INT) AS source_field_decimal_places,
            dd03.POSITION AS source_field_position,
            dd03.DOMNAME AS source_domain_reference, 
            dd03.CHECKTABLE AS source_fk_reference,
            dd04.SCRTEXT_S AS source_short_description, 
            dd04.SCRTEXT_M AS source_medium_description, 
            dd04.SCRTEXT_L AS source_long_description, 
            NULL AS vivid_include_in_silver_flag, 
            -- transform description by removing special characters and replacing spaces with underscores
            vivid_meta.vivid_meta.get_derived_name(dd04.SCRTEXT_L) AS vivid_derived_field_name, 
            NULL AS vivid_user_defined_field_name, 
            NULL AS vivid_field_comment, 
            NULL AS vivid_foreign_key_table_field,
            dtm.vivid_target_datatype AS vivid_field_type, 
            NULL AS vivid_field_length
        FROM vivid_meta.vivid_meta.vivid_table vt
        INNER JOIN {env_vars.bronze_catalog}.sap_s4hana.dd03l AS dd03
            ON UPPER(vt.source_table_name) = dd03.TABNAME
        INNER JOIN {env_vars.bronze_catalog}.sap_s4hana.dd04t AS dd04
            ON dd03.ROLLNAME = dd04.ROLLNAME
        INNER JOIN vivid_meta.vivid_meta.vivid_datatype_mapping AS dtm
            ON dd03.DATATYPE = dtm.vivid_source_datatype
        WHERE vt.source_system = "sap_s4hana"
    ),

    vivid_sac_fields AS (
        -- get the list of table names with other source system
        WITH sac_tables AS (
            SELECT DISTINCT source_system, source_table_name 
            FROM vivid_meta.vivid_meta.vivid_table
            WHERE source_system IN (
                'sap_sac',
                'sap_datasphere',
                'ftp_vbox'
            )
        )

        -- using the information schema get the column names of the other source system tables
        SELECT
            t.source_system AS source_system,
            c.table_name AS source_table_name,
            c.column_name AS source_field_name,
            NULL AS source_field_primary_key_flag,
            c.data_type AS source_data_type,
            NULL AS source_field_length,
            NULL AS source_field_decimal_places,
            c.ordinal_position AS source_field_position,
            NULL AS source_domain_reference, 
            NULL AS source_fk_reference,
            NULL AS source_short_description, 
            NULL AS source_medium_description, 
            NULL AS source_long_description, 
            NULL AS vivid_include_in_silver_flag, 
            vivid_meta.vivid_meta.get_derived_name(c.column_name) AS vivid_derived_field_name, 
            NULL AS vivid_user_defined_field_name, 
            NULL AS vivid_field_comment, 
            NULL AS vivid_foreign_key_table_field, 
            NULL AS vivid_field_type, 
            NULL AS vivid_field_length
        FROM {env_vars.bronze_catalog}.information_schema.columns AS c
        INNER JOIN sac_tables AS t
            ON c.table_name = t.source_table_name
    ),

    union_fields AS (
        SELECT * FROM vivid_sap_s4_fields
            UNION ALL
        SELECT * FROM vivid_sac_fields
        ORDER BY source_field_position
    )

    MERGE INTO vivid_meta.vivid_meta.vivid_field AS target
        USING union_fields
        AS source
        ON
            target.source_system = source.source_system AND
            target.source_table_name = source.source_table_name AND
            target.source_field_name = source.source_field_name
        WHEN NOT MATCHED THEN
        INSERT(
            source_system,
            source_table_name,
            source_field_name,
            source_field_primary_key_flag,
            source_data_type,
            source_field_length,
            source_field_decimal_places,
            source_field_position,
            source_domain_reference,
            source_fk_reference,
            source_short_description,
            source_medium_description,
            source_long_description, 
            vivid_include_in_silver_flag,
            vivid_derived_field_name,
            vivid_user_defined_field_name,
            vivid_field_comment,
            vivid_foreign_key_table_field,
            vivid_field_type,
            vivid_field_length
        )
        VALUES(
            source.source_system,
            source.source_table_name,
            source.source_field_name,
            source.source_field_primary_key_flag,
            source.source_data_type,
            source.source_field_length,
            source.source_field_decimal_places,
            source.source_field_position,
            source.source_domain_reference,
            source.source_fk_reference,
            source.source_short_description,
            source.source_medium_description,
            source.source_long_description,
            source.vivid_include_in_silver_flag,
            source.vivid_derived_field_name,
            source.vivid_user_defined_field_name,
            source.vivid_field_comment,
            source.vivid_foreign_key_table_field,
            source.vivid_field_type,
            source.vivid_field_length
        );
''')