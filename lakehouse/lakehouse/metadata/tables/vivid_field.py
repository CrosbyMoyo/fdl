# Databricks notebook source
spark.sql('''
    CREATE OR REPLACE TABLE vivid_meta.vivid_meta.vivid_field (
        source_system STRING NOT NULL,
        source_table_name STRING NOT NULL,
        source_field_name STRING NOT NULL,
        source_field_primary_key_flag BOOLEAN,
        source_data_type STRING,
        source_field_length INT,
        source_field_decimal_places INT,
        source_field_position STRING,
        source_domain_reference STRING,
        source_fk_reference STRING,
        source_short_description STRING,
        source_medium_description STRING,
        source_long_description STRING,
        vivid_include_in_silver_flag BOOLEAN,
        vivid_derived_field_name STRING,
        vivid_user_defined_field_name STRING,
        vivid_field_comment STRING,
        vivid_foreign_key_table_field STRING,
        vivid_field_type STRING,
        vivid_field_length STRING,
        vivid_primary_key_flag BOOLEAN,
        CONSTRAINT pk_vivid_field PRIMARY KEY (source_system, source_table_name, source_field_name)
    );
''')