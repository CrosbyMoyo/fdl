# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

# MAGIC %run ../../../common/gold_metadata

# COMMAND ----------

metadata_filename = "gold.gross_government_receivables.yaml"
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = GoldMetadataYaml(
    file_path = f'./metadata/{metadata_filename}',
    slv_catalog = env_vars.silver_catalog,
    gld_catalog = env_vars.gold_catalog
)

# COMMAND ----------

destination = metadata.dest_3partname(True)

# COMMAND ----------

spark.sql(f'''
SELECT
    er_ma_filtered.from_currency
    ,er_ma_filtered.to_currency
    ,er_ma_filtered.valid_from
    ,max(er_ma_filtered.fx_rate_avg_monthly) AS fx_rate_avg_monthly
FROM
(
    SELECT DISTINCT
        er.from_currency
        ,er.to_currency
        ,er.valid_from
        ,scaled_exchange_rate as fx_rate_avg_monthly
    FROM
        {env_vars.silver_catalog}.fin_finance.exchange_rates as er
    WHERE
        er.to_currency = 'USD'
        AND er.exchange_rate_type = 'E'
        AND er.valid_from = last_day(er.valid_from)
) AS er_ma_filtered
GROUP BY
    ALL
''').createOrReplaceTempView('fx_er_avg_monthly')

# COMMAND ----------

 spark.sql(f'''
    SELECT
        dpic.currency_key
        ,dpic.currency_decimal_places
    FROM
        {env_vars.silver_catalog}.fin_controlling.decimal_places_in_currencies AS dpic
    WHERE
        dpic.__etl_is_active = True
''').createOrReplaceTempView('decimal_places_in_currencies')

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE TEMP VIEW actual_hana_transactions AS
SELECT 
    ujli.fiscal_year_period, 
    ujli.posting_period,
    ujli.fiscal_year, 
    ujli.company_code, 
    ujli.offsetting_account_number,
    ujli.document_type,
    ujli.gl_account, 
    if(
            ujli.vcode NOT IN ('V.001', 'V.002') and ujli.actual_plan_code = 'Actual' and ujli.datasource NOT IN ('SGR', 'SGRJNLS', 'HFM'),
            ujli.amount_in_company_code_currency / power(10, coalesce(dp.currency_decimal_places, 2) - 2),
            ujli.amount_in_company_code_currency
        ) AS amount_in_company_code_currency,
    ujli.ledger
FROM 
    {env_vars.silver_catalog}.fin_general_ledger.universal_journal_line_items ujli
LEFT JOIN 
    {env_vars.silver_catalog}.fin_general_ledger.gl_account_master gam
ON 
    ujli.gl_account = gam.gl_account
LEFT JOIN 
    decimal_places_in_currencies dp
ON 
    ujli.company_code_currency = dp.currency_key
WHERE 
    actual_plan_code IN ('Actual', 'ACTUAL')
    AND gam.government_receivable_flag = true
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC Aux view for all fiscal year period / company code combinations

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE TEMP VIEW fiscal_year_periods AS
SELECT DISTINCT 
    posting_period,
    fiscal_year_period, 
    fiscal_year 
FROM 
    actual_hana_transactions
"""
)

spark.sql(
    f"""
CREATE OR REPLACE TEMP VIEW company_codes AS
SELECT DISTINCT 
    company_code,
    currency_key
FROM 
    {env_vars.silver_catalog}.ca_cross_application_components.company_code
"""
)

spark.sql(
    f"""
CREATE OR REPLACE TEMP VIEW all_combinations AS
SELECT 
    fyp.posting_period,
    fyp.fiscal_year_period, 
    fyp.fiscal_year,
    cc.company_code,
    cc.currency_key
FROM 
    fiscal_year_periods fyp
CROSS JOIN 
    company_codes cc
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate YTD Amount (only for government receivables) for all possible company code and periods

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW transactional_amounts_view AS
    SELECT 
        ac.fiscal_year_period,
        ac.company_code,
        ac.currency_key,
        ac.fiscal_year,
        ac.posting_period,
        SUM(aht.amount_in_company_code_currency) * -1 as transactional_amounts
    FROM 
        all_combinations ac
    LEFT JOIN
        actual_hana_transactions aht
    ON
        ac.posting_period = aht.posting_period
        AND ac.fiscal_year_period = aht.fiscal_year_period 
        AND ac.company_code = aht.company_code
    GROUP BY
        ac.company_code, 
        ac.fiscal_year_period,
        ac.currency_key,
        ac.fiscal_year,
        ac.posting_period
"""
)

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE TEMP VIEW sum_amounts AS
SELECT 
    fiscal_year_period,
    fiscal_year,
    posting_period,
    company_code, 
    currency_key,
    MAX(sum_amount_in_company_code_currency) as ytd_amounts_in_company_code_currency
FROM (
    SELECT 
        ac.fiscal_year_period,
        ac.company_code,
        ac.currency_key,
        ac.fiscal_year,
        ac.posting_period,
        SUM(COALESCE(aht.amount_in_company_code_currency, 0)) 
            OVER (
                PARTITION BY ac.company_code, ac.fiscal_year 
                ORDER BY ac.fiscal_year_period 
            ) AS sum_amount_in_company_code_currency
    FROM 
        all_combinations ac
    LEFT JOIN
        actual_hana_transactions aht
    ON
        ac.posting_period = aht.posting_period
        AND ac.fiscal_year_period = aht.fiscal_year_period 
        AND ac.company_code = aht.company_code
) 
GROUP BY 
    fiscal_year_period,
    company_code,
    currency_key,
    fiscal_year,
    posting_period
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate YTD Payments (All govt receivables in specific offsetting account number/document_type/company_code setting)

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE TEMP VIEW sum_payments AS
SELECT 
    fiscal_year_period, 
    company_code, 
    fiscal_year,
    posting_period,
    currency_key,
    MAX(sum_payments_in_company_code_currency) as ytd_payments_in_company_code_currency
FROM (
    SELECT 
        ac.fiscal_year_period,
        ac.fiscal_year,
        ac.posting_period,
        ac.currency_key,
        ac.company_code,
        SUM(COALESCE(
            CASE 
                WHEN (aht.offsetting_account_number IN (
                    '0650000023', '0650000320', '0650000440', '0650001022', '0650001032', 
                    '0650001042', '0650001062', '0650001063', '0650001303', '0650001353', 
                    '0650001823', '0650005873', '0650005943', '0650006033'
                ))
                OR (aht.offsetting_account_number IN ('0620700120') AND ac.company_code = 'CI01')
                OR (aht.document_type NOT IN ('WE', 'RE') AND ac.company_code = 'SN01')
                THEN aht.amount_in_company_code_currency
                ELSE NULL
            END, 0)
        ) OVER (
            PARTITION BY ac.company_code, ac.fiscal_year 
            ORDER BY ac.fiscal_year_period 
        ) AS sum_payments_in_company_code_currency
    FROM 
        all_combinations ac
    LEFT JOIN
        actual_hana_transactions aht
    ON
        ac.posting_period = aht.posting_period
        AND ac.fiscal_year_period = aht.fiscal_year_period 
        AND ac.company_code = aht.company_code
) 
GROUP BY
    company_code, 
    fiscal_year_period,
    fiscal_year,
    posting_period,currency_key
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate YTD Offsets (across all transactions) for all company code / fiscal year period combinations

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE TEMP VIEW sum_offsets AS
SELECT 
    company_code,
    fiscal_year_period,
    fiscal_year,
    posting_period,
    MAX(sum_offsets_in_company_currency) AS ytd_offsets_in_company_code_currency
FROM (
    SELECT 
        src.company_code,
        src.fiscal_year_period,
        src.fiscal_year,
        src.posting_period,
        SUM(
            CASE 
                WHEN src.supplier IN ('0001060707', '0001375684', '0001219144', '0001060627', '0001060649', '0001060653')
                     AND src.account_type = 'K'
                THEN src.amount_in_company_code_currency_decimal
                ELSE NULL
            END
        ) OVER (
            PARTITION BY src.company_code, src.fiscal_year
            ORDER BY src.fiscal_year_period 
        ) AS sum_offsets_in_company_currency
    FROM (
        SELECT 
            ujli.*,
            IF(
                ujli.vcode NOT IN ('V.001', 'V.002') 
                AND ujli.actual_plan_code = 'Actual' 
                AND ujli.datasource NOT IN ('SGR', 'SGRJNLS', 'HFM'),
                ujli.amount_in_company_code_currency / POWER(10, COALESCE(dp.currency_decimal_places, 2) - 2),
                ujli.amount_in_company_code_currency
            ) AS amount_in_company_code_currency_decimal
        FROM 
            {metadata.alias2src('ujli')} AS ujli
        LEFT JOIN 
            decimal_places_in_currencies dp
        ON 
            ujli.company_code_currency = dp.currency_key
    ) src
    WHERE 
        src.actual_plan_code = 'Actual'
        AND src.posting_period NOT IN ('13')
)
GROUP BY 
    company_code,
    fiscal_year_period,
    fiscal_year,
    posting_period
"""
)

# COMMAND ----------

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW combined_sums AS
    SELECT 
        a.fiscal_year_period,
        a.fiscal_year,
        a.posting_period,
        a.company_code,
        a.currency_key,
        a.ytd_amounts_in_company_code_currency * -1 as ytd_amounts_in_company_code_currency,
        b.ytd_offsets_in_company_code_currency * -1 as ytd_offsets_in_company_code_currency,
        c.ytd_payments_in_company_code_currency * -1 as ytd_payments_in_company_code_currency,
        d.transactional_amounts
    FROM 
        sum_amounts a
    LEFT JOIN 
        sum_offsets b
    ON 
        a.company_code = b.company_code
        AND a.fiscal_year_period = b.fiscal_year_period
    LEFT JOIN 
        sum_payments c
    ON 
        a.company_code = c.company_code
        AND a.fiscal_year_period = c.fiscal_year_period
    LEFT JOIN
        transactional_amounts_view d
    ON
        a.company_code = d.company_code
        AND a.fiscal_year_period = d.fiscal_year_period
    """
)

# COMMAND ----------

spark.sql(f'''SELECT 
    f.*,
    concat(substr(fiscal_year_period, 1, 4), '-', substr(fiscal_year_period, 6, 2)) AS year_month,
    'USD' AS global_currency, 
    fx.fx_rate_avg_monthly,
    month(fx.valid_from) as fx_rate_month,
    f.ytd_amounts_in_company_code_currency * fx.fx_rate_avg_monthly AS ytd_amounts_in_global_currency,
    f.ytd_offsets_in_company_code_currency * fx.fx_rate_avg_monthly AS ytd_offsets_in_global_currency,
    f.ytd_payments_in_company_code_currency * fx.fx_rate_avg_monthly AS ytd_payments_in_global_currency
FROM 
    combined_sums f
LEFT JOIN 
    fx_er_avg_monthly fx
ON 
    (f.fiscal_year = YEAR(fx.valid_from)
AND
    f.posting_period = MONTH(fx.valid_from))
AND
    fx.from_currency = f.currency_key
WHERE 
    posting_period <> 0
''').createOrReplaceTempView('fx_applied_view')

# COMMAND ----------

hashed_gold_table = spark.sql(f'''{metadata.get_etl_fields_ddl('fx_applied_view')}''')

hashed_gold_table.createOrReplaceTempView('hashed_gold_table')

# COMMAND ----------

metadata.insert_overwrite('hashed_gold_table', destination)