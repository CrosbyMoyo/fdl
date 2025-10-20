# Databricks notebook source
spark.sql('''
      CREATE OR REPLACE TABLE vivid_meta.vivid_meta.vivid_table (
            source_system STRING NOT NULL,
            source_table_name STRING NOT NULL,
            source_table_description STRING,
            vivid_schema STRING,
            vivid_derived_table_name STRING,
            vivid_user_defined_table_name STRING,
            vivid_table_comment STRING,
            vivid_filter_condition STRING,
            CONSTRAINT pk_vivid_table PRIMARY KEY (source_system, source_table_name)
      );
''')