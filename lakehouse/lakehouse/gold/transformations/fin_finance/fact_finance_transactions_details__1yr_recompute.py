# Databricks notebook source
# MAGIC %md
# MAGIC Notebook recomputes the `amount_group_currency_plus_1_year_rate` field 1 year on from when the record was created.
# MAGIC
# MAGIC Overwrites it with the newer exchange rate (e.g. for Feb 2024 it updates the field with the value from Feb 2025)
# MAGIC
# MAGIC This Notebook is called on the same Workflow to the hourly load - it only needs to be run on the first of the month (e.g. for Feb 2025 it would be run on 1-Mar-2025)
# MAGIC

# COMMAND ----------

# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from delta import DeltaTable

# COMMAND ----------

dbutils.widgets.text(
    name='metadata_filename',
    defaultValue='',
    label='1 - metadata_filename'
)

dbutils.widgets.text(
    name='run_date',
    defaultValue='',
    label='2 - Run Date'
)

# COMMAND ----------

metadata_filename = dbutils.widgets.get('metadata_filename')
assert metadata_filename, 'metadata_filename must be provided'

logger.log.info(f'Widget: metadata_filename = "{metadata_filename}"')

# COMMAND ----------

# based on the run_date get the relative dates for the last year
# these are used for *last year's* data

run_date = datetime.strptime(dbutils.widgets.get("run_date"), '%Y-%m-%d').date()

last_year = run_date - relativedelta(years=1)

data_start_date = (
    (last_year.replace(day=1) - timedelta(days=1))
    .replace(day=1)
    .strftime("%Y-%m-%d")
)

data_end_date = (
    (last_year.replace(day=1) - timedelta(days=1))
    .strftime("%Y-%m-%d")
)

logger.log.info(f'data_start_date = "{data_start_date}", data_end_date = "{data_end_date}"')

# COMMAND ----------

# based on the run_date get the first and last of the previous month, and the start of the year
# these are used for *this year's* exchange rates

er_start_date = (
    (run_date.replace(day=1) - timedelta(days=1))
    .replace(day=1)
    .strftime("%Y-%m-%d")
)

er_end_date = (
    (run_date.replace(day=1) - timedelta(days=1))
    .strftime("%Y-%m-%d")
)

er_start_of_year = run_date.replace(month=1, day=1).strftime("%Y-%m-%d")

logger.log.info(f'er_start_date = "{er_start_date}", er_end_date = "{er_end_date}", er_start_of_year = "{er_start_of_year}"')

# COMMAND ----------

metadata = GoldMetadataYaml(
    file_path = f'./metadata/{metadata_filename}',
    slv_catalog = env_vars.silver_catalog,
    gld_catalog = env_vars.gold_catalog
)

# COMMAND ----------

# get this year's exchange rate values

# COMMAND ----------

exchange_rates = spark.sql(f'''
    SELECT
        er_filtered.from_currency
        ,max(er_filtered.fx_rate_avg_monthly) AS fx_rate_avg_monthly
        ,max(er_filtered.fx_rate_month_end) AS fx_rate_month_end
        ,max(er_filtered.fx_rate_planning) AS fx_rate_planning
    FROM
    (
        SELECT DISTINCT
            er.from_currency
            ,er.to_currency
            ,er.valid_from
            ,CASE
                WHEN er.exchange_rate_type = 'I' THEN er.scaled_exchange_rate
                ELSE NULL
            END AS fx_rate_avg_monthly
            ,CASE
                WHEN er.exchange_rate_type = 'E' THEN er.scaled_exchange_rate
                ELSE NULL
            END AS fx_rate_month_end
            ,CASE
                WHEN er.exchange_rate_type = 'PA' THEN er.scaled_exchange_rate
                ELSE NULL
            END AS fx_rate_planning
        FROM
            {metadata.alias2src('er')} AS er
        WHERE
            er.to_currency = 'USD'
            AND er.exchange_rate_type IN (
                'I',
                'E',
                'PA'
            )
            AND (
                (er.valid_from BETWEEN '{er_start_date}' AND '{er_end_date}')
                OR er.valid_from = '{er_start_of_year}'
            )
    ) AS er_filtered
    GROUP BY
        ALL
''')

exchange_rates.createOrReplaceTempView('exchange_rates')


# COMMAND ----------

# calculate last year's group currency amount using this year's exchange rates

# COMMAND ----------

ftd_name = metadata.dest_3partname(True)

# COMMAND ----------

recalc_ftd = spark.sql(f'''

    SELECT
        ftd.fact_finance_transaction_details_skey   
        ,ftd.date_key
        ,ftd.actual_plan_code
        ,ftd.local_currency_code
        ,ftd.amount_local_currency
        ,ftd.fx_rate_avg_monthly AS old_fx_rate_avg_monthly
        ,ftd.fx_rate_month_end AS old_fx_rate_month_end
        ,ftd.fx_rate_planning AS old_fx_rate_planning
        ,ftd.balance_sheet_account_flag
        ,ftd.amount_group_currency
        ,'---' AS brk1
        ,er.fx_rate_avg_monthly AS new_fx_rate_avg_monthly
        ,er.fx_rate_month_end AS new_fx_rate_month_end
        ,er.fx_rate_planning AS new_fx_rate_planning
        ,'---' AS brk2
        ,CAST(
            CASE
                WHEN ftd.actual_plan_code <> 'Actual'
                    THEN ftd.amount_local_currency * er.fx_rate_planning
                WHEN ftd.balance_sheet_account_flag = True
                    THEN ftd.amount_local_currency * er.fx_rate_month_end
                ELSE
                    ftd.amount_local_currency * er.fx_rate_avg_monthly
            END AS DECIMAL(18, 4)
        ) AS amount_group_currency_plus_1_year_rate
        ,'---' AS brk3
        ,ftd.amount_group_currency_plus_1_year_rate AS old_amount_group_currency_plus_1_year_rate
        ,ftd.__etl_fprint
    FROM
        {ftd_name} AS ftd
        LEFT JOIN  exchange_rates AS er
            ON ftd.local_currency_code = er.from_currency
    WHERE
        ftd.date_key BETWEEN "{data_start_date}" AND "{data_end_date}"

''')

recalc_ftd.createOrReplaceTempView('recalc_ftd')

# COMMAND ----------

# write the recalculated value back to the Fact table

# COMMAND ----------

merge_result = spark.sql(f'''

    MERGE INTO {ftd_name} AS tgt
    USING recalc_ftd AS src
        ON tgt.fact_finance_transaction_details_skey  = src.fact_finance_transaction_details_skey 
    WHEN MATCHED THEN
        UPDATE SET
            tgt.amount_group_currency_plus_1_year_rate = src.amount_group_currency_plus_1_year_rate

''')

# COMMAND ----------

logger.log.info(f'Merge: {metadata.dest_3partname(True)} {merge_result.toPandas().head(1)}')