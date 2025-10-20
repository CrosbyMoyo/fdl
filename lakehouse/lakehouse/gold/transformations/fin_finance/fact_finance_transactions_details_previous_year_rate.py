# Databricks notebook source
# MAGIC %md
# MAGIC Notebook recomputes the `amount_group_currency_plus_1_year_rate` field 1 year on from when the record was created.
# MAGIC
# MAGIC Overwrites it with the newer exchange rate (e.g. for Feb 2024 it updates the field with the value from Feb 2025)
# MAGIC
# MAGIC This Notebook is called on a separate Workflow to the hourly load - it only needs to be run on the first of the month (e.g. for Feb 2025 it would be run on 1-Mar-2025)
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

# COMMAND ----------

metadata_filename = dbutils.widgets.get('metadata_filename')
assert metadata_filename, 'metadata_filename must be provided'

logger.log.info(f'Widget: metadata_filename = "{metadata_filename}"')

# COMMAND ----------

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

today_date = datetime.now().date()
previous_year_date = today_date - relativedelta(years=1)
data_end_date = (previous_year_date.replace(day=1) - timedelta(days=1)).strftime("%Y-%m-%d")

er_end_date = (today_date.replace(day=1) - timedelta(days=1)).strftime("%Y-%m-%d")

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
    WITH yearly_rates AS (
        SELECT DISTINCT
            er.from_currency
            ,er.to_currency
            ,er.valid_from
            ,date_format(er.valid_from, 'yyyy') AS year
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
            AND date_format(er.valid_from, 'MM-dd') = '01-01'
    )
    SELECT
        er_filtered.from_currency
        ,er_filtered.year_month
        ,GREATEST(max(er_filtered.fx_rate_avg_monthly), max(yr.fx_rate_avg_monthly)) AS fx_rate_avg_monthly
        ,GREATEST(max(er_filtered.fx_rate_month_end), max(yr.fx_rate_month_end)) AS fx_rate_month_end
        ,GREATEST(max(er_filtered.fx_rate_planning), max(yr.fx_rate_planning)) AS fx_rate_planning
    FROM
    (
        SELECT DISTINCT
            er.from_currency
            ,er.to_currency
            ,er.valid_from
            ,date_format(er.valid_from, 'yyyy-MM') AS year_month
            ,date_format(er.valid_from, 'yyyy') AS year
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
                er.valid_from <= '{er_end_date}'
            )
    ) AS er_filtered
    LEFT JOIN yearly_rates yr
    ON er_filtered.from_currency = yr.from_currency
    AND er_filtered.year = yr.year
    GROUP BY
        ALL
''')

exchange_rates.createOrReplaceTempView('exchange_rates')

# COMMAND ----------

# calculate last year's group currency amount using this year's exchange rates

# COMMAND ----------

ftd_name = metadata.dest_2partname('fact_finance_transaction_details')

# COMMAND ----------

gold_recalc_ftd = spark.sql(f'''

    SELECT
        ftd.* EXCEPT (amount_group_currency_plus_1_year_rate)
        ,CAST(
            CASE
                WHEN ftd.actual_plan_code <> 'Actual' and ftd.date_key <= "{data_end_date}"
                    THEN ftd.amount_local_currency * er.fx_rate_planning
                WHEN ftd.balance_sheet_account_flag = True and ftd.date_key <= "{data_end_date}"
                    THEN ftd.amount_local_currency * er.fx_rate_month_end
                ELSE
                    ftd.amount_local_currency * er.fx_rate_avg_monthly
            END AS DECIMAL(18, 4)
        ) AS amount_group_currency_plus_1_year_rate,
        er.fx_rate_avg_monthly as old_rate_avg_monthly,
        er.fx_rate_month_end as old_rate_month_end,
        er.fx_rate_planning as old_rate_planning
    FROM
        {env_vars.gold_catalog}.{ftd_name} AS ftd
        LEFT JOIN  exchange_rates AS er
            ON ftd.local_currency_code = er.from_currency
            AND DATE_FORMAT(DATE_ADD(year, 1, ftd.date_key), 'yyyy-MM') = DATE_FORMAT(er.year_month, 'yyyy-MM')

''')

gold_recalc_ftd.createOrReplaceTempView('gold_recalc_ftd')

# COMMAND ----------

result = spark.sql(f'''
    INSERT OVERWRITE {metadata.dest_3partname(True)} BY NAME 
    SELECT
        -- keys
        g.fact_finance_transaction_details_skey
        ,g.date_key
        ,g.profit_center_skey
        ,g.line_of_business_skey
        ,g.company_code_skey
        ,g.country_key
        ,g.vcode_skey
        ,g.cost_center_skey
        ,g.gl_account_skey
        ,g.datasource_skey
        ,g.material_skey
        ,g.local_currency_skey
        ,g.group_currency_skey
        ,g.actual_plan_code

        --measures
        ,g.amount_local_currency
        ,g.local_currency_code
        ,g.amount_group_currency
        ,g.amount_group_currency_plan_rate
        ,g.amount_group_currency_month_end
        ,g.amount_group_currency_plus_1_year_rate
        ,g.volume_litres_l20
        ,g.volume_kg
        ,g.volume_issued_litres_l20
        ,g.vcode_amount_local
        ,g.vcode_amount_group

        -- reconciliation columns
        ,g.document_number
        ,g.posting_item
        ,g.document_status
        ,g.document_type
        ,g.customer
        ,g.bill_to_party
        ,g.ship_to_party
        ,g.material
        ,g.ifrs_flag

        -- fx rates
        ,g.fx_rate_avg_monthly
        ,g.fx_rate_month_end
        ,g.fx_rate_planning
        ,g.balance_sheet_account_flag

        -- metadata
        ,g.__etl_fprint
        ,g.__etl_load_timestamp
        ,g.__etl_is_active
        ,g.__etl_is_deleted
    FROM
        gold_recalc_ftd 
    AS 
        g;
''')