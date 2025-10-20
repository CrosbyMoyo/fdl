# Databricks notebook source
# set up a widget for the SAP table name

# COMMAND ----------

# get that metadata from vivid_meta.vivid_meta

# COMMAND ----------

# set up the template for the CREATE TABLE statement


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   -- PK
# MAGIC   a.RCLNT
# MAGIC   ,a.RLDNR
# MAGIC   ,a.RBUKRS
# MAGIC   ,a.GJAHR
# MAGIC   ,a.BELNR
# MAGIC   ,a.DOCLN
# MAGIC   -- doc date
# MAGIC   ,a.BUDAT
# MAGIC   ,substring(a.BUDAT, 1, 4) AS doc_year
# MAGIC   ,substring(a.BUDAT, 5, 2) AS doc_month
# MAGIC   ,timestamp(
# MAGIC       concat(substring(a.`TIMESTAMP`, 1, 4)
# MAGIC       , '-', substring(a.`TIMESTAMP`, 5, 2)
# MAGIC       , '-', substring(a.`TIMESTAMP`, 7, 2)
# MAGIC       , ' ', substring(a.`TIMESTAMP`, 9, 2)
# MAGIC       , ':', substring(a.`TIMESTAMP`, 11, 2)
# MAGIC       , ':', substring(a.`TIMESTAMP`, 13, 2)
# MAGIC       )
# MAGIC   ) AS last_updated
# MAGIC   ,a.__etl_id
# MAGIC   ,a.__etl_bronze_timestamp
# MAGIC   ,a.__etl_silver_timestamp
# MAGIC FROM
# MAGIC   vivid_dev_brz.sap_s4hana.acdoca AS a
# MAGIC ORDER BY
# MAGIC   last_updated
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM
# MAGIC   vivid_dev_brz.information_schema.columns AS c
# MAGIC WHERE
# MAGIC   c.table_name = 'acdoca'

# COMMAND ----------

# TODO: get a process to pull all the Bronze tables from the metadata, along with column comments, and Tags to say if this is a Key
# Include the standard metadata columns
# Choose datatypes based on the SAP originals
# Preserve the column order

# COMMAND ----------

table_cols = spark.sql('''
    SELECT
        f.source_field_name
        ,f.source_field_primary_key_flag
        ,f.source_data_type
        ,f.source_field_length
        ,f.source_field_decimal_places
        ,f.source_field_position
        ,f.source_long_description
        ,f.vivid_derived_field_name
        ,f.vivid_user_defined_field_name
        ,f.vivid_field_comment
        ,f.vivid_field_type
    FROM
        vivid_meta.vivid_meta.vivid_field AS f
    WHERE
        f.source_table_name = 'acdoca'
        AND f.vivid_include_in_silver_flag = true
    ORDER BY
        f.source_field_position
''')

# COMMAND ----------

cols_list = table_cols.collect()

cols_list[0]['source_field_name']

# COMMAND ----------

catalog_placeholder = '{env_vars.bronze_catalog}'
table_schema = 'sap_s4hana'
table_name = 'acdoca'

create_operation = 'CREATE TABLE IF NOT EXISTS '

table_ddl = f'''
{create_operation} {catalog_placeholder}.{table_schema}.{table_name} (
    
)
CLUSTER BY
    AUTO;
'''

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- get a list of all the Bronze tables
# MAGIC
# MAGIC SELECT
# MAGIC   t.table_schema
# MAGIC   ,t.table_name
# MAGIC   ,t.table_owner
# MAGIC   ,t.created
# MAGIC   ,t.created_by
# MAGIC   ,t.last_altered
# MAGIC   ,t.last_altered_by
# MAGIC FROM
# MAGIC   vivid_dev_brz.information_schema.tables AS t
# MAGIC WHERE
# MAGIC   t.table_schema <> 'information_schema'
# MAGIC ORDER BY
# MAGIC   t.table_schema
# MAGIC   ,t.table_name
# MAGIC