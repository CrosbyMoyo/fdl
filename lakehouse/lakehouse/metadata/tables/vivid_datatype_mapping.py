# Databricks notebook source
spark.sql('''
      CREATE OR REPLACE TABLE vivid_meta.vivid_meta.vivid_datatype_mapping (
            vivid_source_system STRING NOT NULL,
            vivid_source_datatype STRING NOT NULL,
            vivid_target_datatype STRING,
            vivid_simple_or_complex_cast STRING,
            vivid_complex_cast_function STRING,
            vivid_function_parameters STRING,
            CONSTRAINT pk_vivid_datatype_mapping PRIMARY KEY (vivid_source_system, vivid_source_datatype)
      );
''')
