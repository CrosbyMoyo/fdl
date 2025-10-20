# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.gold_catalog}.fin_controlling.fact_health_and_safety_incidents
    (
        -- FKs
        year_month STRING
            COMMENT 'FK to dim_date year_month',
        version_skey BIGINT
            CONSTRAINT fact_health_and_safety_incidents_version_key_fk
            FOREIGN KEY REFERENCES {env_vars.gold_catalog}.ca_cross_application_components.dim_version (version_skey)
            COMMENT 'FK to dim_version',


        -- PAYLOAD
        life_saving_rule_violations DOUBLE
            COMMENT 'life saving rule violations',
        lost_time_injury_frequency DOUBLE
            COMMENT 'lost time injury frequency',
        potential_incidents DOUBLE
            COMMENT 'potential incidents',
        severe_motor_vehicle_incident_rate DOUBLE
            COMMENT 'severe motor vehicle incident rate',
        spills_100kg DOUBLE
            COMMENT 'spills 100kg',
        fatalities DOUBLE
            COMMENT 'fatalities',
        spills DOUBLE
            COMMENT 'spills',
        recordable_case_frequency_cont DOUBLE
            COMMENT 'recordable case frequency cont',
        recordable_case_frequency_emp DOUBLE
            COMMENT 'recordable case frequency emp',

        -- metadata
        __etl_fprint BIGINT
            COMMENT 'xxhash64 of the columns that make up this row: FKs and payload combined',
        __etl_load_timestamp TIMESTAMP
            COMMENT 'datetime that the row was added to the table',
        __etl_is_active BOOLEAN
            COMMENT 'flag indicating the active record. Note: there should only be 1 _is_active for any _etl_fprint',
        __etl_is_deleted BOOLEAN
            COMMENT 'flag showing if the record has been deleted from the source system'

    )
    CLUSTER BY AUTO;

''')

# COMMAND ----------

