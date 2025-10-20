# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

from datetime import datetime, timezone

load_timestamp = datetime.now(timezone.utc)

# COMMAND ----------

dbutils.widgets.text(
    name='metadata_filename',
    defaultValue='',
    label='1 - metadata_filename'
)

# COMMAND ----------

metadata_filename = dbutils.widgets.get('metadata_filename')
logger.log.info(f'Widget: metadata_filename = "{metadata_filename}"')

# COMMAND ----------

metadata = GoldMetadataYaml(
    file_path = f'./metadata/{metadata_filename}',
    slv_catalog = env_vars.silver_catalog,
    gld_catalog = env_vars.gold_catalog
)

# COMMAND ----------

spark.sql(
    f'''
        SELECT
            -- Foreign Keys 
            f.year_month
            ,{metadata.get_fkey_ddl(['f.actual_target'])} AS version_skey
            
            -- PAYLOAD
            ,f.life_saving_rule_violations
            ,f.lost_time_injury_frequency
            ,f.potential_incidents
            ,f.severe_motor_vehicle_incident_rate
            ,f.spills_100kg
            ,f.fatal_incidents AS fatalities
            ,f.spills
            ,f.recordable_case_frequency_cont
            ,f.recordable_case_frequency_emp

            --metadata
            ,0 AS __etl_fprint
            ,'{load_timestamp}' AS __etl_load_timestamp
            ,True AS __etl_is_active
            ,False AS __etl_is_deleted
        FROM 
            {metadata.alias2src('hsseq')} AS f
    '''
).createOrReplaceTempView('gold_table')

# COMMAND ----------

spark.sql(
    f'''
        INSERT OVERWRITE {metadata.dest_3partname(True)}
        (
            -- keys
            year_month
            ,version_skey

            -- measures
            ,life_saving_rule_violations
            ,lost_time_injury_frequency
            ,potential_incidents
            ,severe_motor_vehicle_incident_rate
            ,spills_100kg
            ,fatalities
            ,spills
            ,recordable_case_frequency_cont
            ,recordable_case_frequency_emp

            -- metadata
            ,__etl_fprint
            ,__etl_load_timestamp
            ,__etl_is_active
            ,__etl_is_deleted
        )
        SELECT
            -- Foreign Keys 
            g.year_month
            ,g.version_skey
            
            -- PAYLOAD
            ,g.life_saving_rule_violations
            ,g.lost_time_injury_frequency
            ,g.potential_incidents
            ,g.severe_motor_vehicle_incident_rate
            ,g.spills_100kg
            ,g.fatalities
            ,g.spills
            ,g.recordable_case_frequency_cont
            ,g.recordable_case_frequency_emp

            --metadata
            ,0
            ,g.__etl_load_timestamp
            ,g.__etl_is_active
            ,g.__etl_is_deleted

        FROM
            gold_table g
    '''
)

# COMMAND ----------

