# Databricks notebook source
spark.sql('''
    INSERT INTO vivid_meta.vivid_meta.vivid_datatype_mapping (
        vivid_source_system, 
        vivid_source_datatype, 
        vivid_target_datatype, 
        vivid_simple_or_complex_cast, 
        vivid_complex_cast_function,
        vivid_function_parameters
    ) VALUES 
        ('sap_s4hana', 'SSTRING', 'STRING', "simple", NULL, NULL),
        ('sap_s4hana', 'TIMS', 'TIMESTAMP', "complex", 'ca_cross_application_components.extract_sap_timestamp', "sap_timestamp: STRING"),
        ('sap_s4hana', 'STRING', 'STRING', "simple", NULL, NULL),
        ('sap_s4hana', 'INT4', 'INT', "simple", NULL, NULL),
        ('sap_s4hana', 'LCHAR', 'STRING', "simple", NULL, NULL),
        ('sap_s4hana', 'NUMC', 'INT', "simple", NULL, NULL),
        ('sap_s4hana', 'CLNT', 'STRING', "simple", NULL, NULL),
        ('sap_s4hana', 'CUKY', 'STRING', "simple", NULL, NULL),
        ('sap_s4hana', 'CHAR', 'STRING', "simple", NULL, NULL),
        ('sap_s4hana', 'UNIT', 'STRING', "simple", NULL, NULL),
        ('sap_s4hana', 'LANG', 'STRING', "simple", NULL, NULL),
        ('sap_s4hana', 'ACCP', 'STRING', "simple", NULL, NULL),
        ('sap_s4hana', 'DEC', 'DECIMAL(u,z)', "simple", NULL, NULL),
        ('sap_s4hana', 'CURR', 'DECIMAL(u,z)', "simple", NULL, NULL),
        ('sap_s4hana', 'FLTP', 'DECIMAL(u,z)', "simple", NULL, NULL),
        ('sap_s4hana', 'INT2', 'INT', "simple", NULL, NULL),
        ('sap_s4hana', 'QUAN', 'DECIMAL(u,z)', "simple", NULL, NULL),
        ('sap_s4hana', 'DATS', 'DATE', "complex", 'ca_cross_application_components.extract_sap_date', "sap_date: STRING"),
        ('sap_s4hana', 'INT1', 'INT', "simple", NULL, NULL);
''')

