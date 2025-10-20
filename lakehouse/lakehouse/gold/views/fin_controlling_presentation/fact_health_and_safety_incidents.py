# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE VIEW {env_vars.gold_catalog}.fin_controlling_presentation.fact_health_and_safety_incidents
    AS
    SELECT
        f.year_month,
        f.version_skey,
        f.life_saving_rule_violations,
        f.lost_time_injury_frequency,
        f.potential_incidents,
        f.severe_motor_vehicle_incident_rate,
        f.spills_100kg,
        f.fatalities,
        f.spills,
        f.recordable_case_frequency_cont,
        f.recordable_case_frequency_emp
    FROM
        {env_vars.gold_catalog}.fin_controlling.fact_health_and_safety_incidents AS f;
''')

# COMMAND ----------

if env_vars.env == 'dev':
    spark.sql(f'''
        GRANT ALL PRIVILEGES
        ON VIEW {env_vars.gold_catalog}.fin_controlling_presentation.fact_health_and_safety_incidents
        TO `data-engineers`;
    ''')