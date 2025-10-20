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
    defaultValue='gold.fact.finance_transactions_details.yaml',
    label='1 - metadata_filename'
)

# COMMAND ----------

metadata_filename = dbutils.widgets.get('metadata_filename')
assert metadata_filename, 'metadata_filename must be provided'

logger.log.info(f'Widget: metadata_filename = "{metadata_filename}"')

# COMMAND ----------

metadata = GoldMetadataYaml(
    file_path = f'./metadata/{metadata_filename}',
    slv_catalog = env_vars.silver_catalog,
    gld_catalog = env_vars.gold_catalog
)

# COMMAND ----------

destination = metadata.dest_3partname(True)

# COMMAND ----------

company_code = spark.sql(f'''
    SELECT
        compc.company_code
        ,compc.country_key
    FROM
        {metadata.alias2src('compc')} AS compc
    WHERE
        compc.__etl_is_active = True
''')

company_code.createOrReplaceTempView('company_code')

# COMMAND ----------

decimal_places_in_currencies = spark.sql(f'''
    SELECT
        dpic.currency_key
        ,dpic.currency_decimal_places
    FROM
        {metadata.alias2src('dpic')} AS dpic
    WHERE
        dpic.__etl_is_active = True
''')

decimal_places_in_currencies.createOrReplaceTempView('decimal_places_in_currencies')

# COMMAND ----------

exchange_rates = spark.sql(f'''
    SELECT
        er_filtered.from_currency
        ,er_filtered.to_currency
        ,er_filtered.valid_from
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
    ) AS er_filtered
    GROUP BY
        ALL
''')

exchange_rates.createOrReplaceTempView('exchange_rates')

# COMMAND ----------

plan_filtered = spark.sql(
    f'''
        SELECT 
            ftp.* 
        FROM
            {metadata.alias2src('ftp')} AS ftp
        WHERE 
            ftp.profit_center <> ''
    '''
)

plan_filtered.createOrReplaceTempView('plan_filtered')

# COMMAND ----------

ujli_unioned = spark.sql(
    f'''
            SELECT
                ujli.actual_plan_code AS actual_plan_code,
                ujli.posting_date AS posting_date,  
                last_day(ujli.posting_date) AS posting_date_last_day,
                date_add(ujli.posting_date, (date_part('DOY', ujli.posting_date) * -1) + 1) AS posting_date_start_of_year,
                ujli.datasource,
                ujli.journal_type,
                ujli.gl_account,
                ujli.chart_of_accounts,
                ujli.company_code,
                ujli.cost_center,
                ujli.profit_center,
                ujli.ifrs_flag,
                ujli.material,
                ujli.customer,
                ujli.segment,
                ujli.gl_account_type,
                ujli.line_of_business,
                ujli.line_of_business_1,
                ujli.vcode,
                ujli.document_number,
                ujli.posting_item,
                ujli.controlling_area,
                ujli.division,
                ujli.asset,
                ujli.payer,
                ujli.sales_organization,
                ujli.ship_to_party,
                ujli.bill_to_party,
                ujli.distribution_channel,
                ujli.plant,
                ujli.subitem,
                ujli.supplier,
                ujli.purchasing_document_item,
                ujli.sales_order,
                ujli.document_date,
                ujli.document_type,
                ujli.document_status,
                ujli.sales_order_item,
                ujli.invoice_reference,
                ujli.item_category,
                ujli.subitem_category,
                ujli.purchasing_document,
                ujli.debit_credit_ind,
                ujli.financial_statement_item,
                ujli.ledger,
                ujli.business_area,
                ujli.base_unit_of_measure,
                ujli.additional_unit_of_measure_1,
                ujli.additional_unit_of_measure_2,
                ujli.reference_document,
                ujli.transaction_type,
                ujli.reference_org_unit,
                ujli.transaction_type_gl,
                ujli.reference_document_line_item,
                ujli.product_sold_group,
                ujli.debit_credit_description,
                ujli.balance_sheet_account_flag,
                ujli.transaction_currency,
                ujli.company_code_currency,
                ujli.elimination_flag,
                ujli.quantity,
                ujli.volume_kg,
                ujli.volume_litres_l20,
                ujli.volume_issued_litres_l20,
                ujli.amount_in_company_code_currency,
                ujli.amount_in_global_currency,
                ujli.vcode_amount_local,
                ujli.vcode_amount_group
            FROM
                {metadata.alias2src('ujli')} AS ujli
        UNION ALL
            SELECT
                ftp.plan_version,
                ftp.date_key AS posting_date,
                last_day(ftp.date_key) AS posting_date_last_day,
                date_add(ftp.date_key, (date_part('DOY', ftp.date_key) * -1) + 1) AS posting_date_start_of_year,
                'SAC' AS datasource,
                ''    AS journal_type,
                ftp.gl_account,
                ftp.controlling_area AS chart_of_accounts,
                ftp.company_code,
                ftp.cost_center,
                ftp.profit_center,
                False AS ifrs_flag,
                ftp.material,
                ftp.customer,
                '' AS segment,
                '' AS gl_account_type,
                '' AS line_of_business,
                ftp.line_of_business AS line_of_business_1,
                ftp.vcode,
                '' AS document_number,
                '' AS posting_item,
                ftp.controlling_area,
                '' AS division,
                '' AS asset,
                '' AS payer,
                '' AS sales_organization,
                ftp.customer AS ship_to_party,
                ftp.customer AS bill_to_party,
                '' AS distribution_channel,
                '' AS plant,
                '' AS subitem,
                '' AS supplier,
                '' AS purchasing_document_item,
                '' AS sales_order,
                '' AS document_date,
                '' AS document_type,
                '' AS document_status,
                '' AS sales_order_item,
                '' AS invoice_reference,
                '' AS item_category,
                '' AS subitem_category,
                '' AS purchasing_document,
                '' AS debit_credit_ind,
                '' AS financial_statement_item,
                '' AS ledger,
                '' AS business_area,
                '' AS base_unit_of_measure,
                '' AS additional_unit_of_measure_1,
                '' AS additional_unit_of_measure_2,
                '' AS reference_document,
                '' AS transaction_type,
                '' AS reference_org_unit,
                '' AS transaction_type_gl,
                '' AS reference_document_line_item,
                '' AS product_sold_group,
                '' AS debit_credit_description,
                False AS balance_sheet_account_flag,
                '' AS transaction_currency,
                ftp.currency_key,
                '' AS elimination_flag,
                ftp.quantity,
                ftp.volume_kg,
                ftp.volume_litres_l20,
                ftp.volume_issued_litres_l20,
                ftp.amount_local_currency,
                0 AS amount_in_global_currency,
                ftp.vcode_amount_local,
                0 AS vcode_amount_group
            FROM
                plan_filtered AS ftp 
''')

ujli_unioned.createOrReplaceTempView('ujli_unioned')

# COMMAND ----------

ujli_fx = spark.sql(f'''
    SELECT
        ujli.* EXCEPT(
            ujli.amount_in_company_code_currency,
            ujli.vcode_amount_local,
            ujli.vcode_amount_group
        )
        ,CASE
            WHEN ujli.datasource = 'SAC' THEN ujli.vcode_amount_local 
            ELSE ujli.vcode_amount_group
        END AS vcode_amount_group 
        -- NOTE: Default DP is 2 for all currencies unless specified in TCURX
        ,if(
            -- TODO: scaling should be done only in one place
            ujli.vcode NOT IN ('V.001', 'V.002') and ujli.actual_plan_code = 'Actual' and ujli.datasource NOT IN ('SGR', 'SGRJNLS', 'HFM'),
            ujli.vcode_amount_local / power(10, coalesce(dp.currency_decimal_places, 2) - 2),
            ujli.vcode_amount_local
        ) AS vcode_amount_local
        ,if(
            -- TODO: scaling should be done only in one place
            ujli.vcode NOT IN ('V.001', 'V.002') and ujli.actual_plan_code = 'Actual' and ujli.datasource NOT IN ('SGR', 'SGRJNLS', 'HFM'),
            ujli.amount_in_company_code_currency / power(10, coalesce(dp.currency_decimal_places, 2) - 2),
            ujli.amount_in_company_code_currency
        ) AS amount_in_company_code_currency
        ,ujli.amount_in_company_code_currency AS unscaled_amount_in_company_code_currency
        ,power(10, coalesce(dp.currency_decimal_places, 2)) AS tdec
        ,er_month.fx_rate_avg_monthly
        ,er_month.fx_rate_month_end
        ,er_year.fx_rate_planning
        ,coalesce(cc.country_key, '') AS country_key 
    FROM
        ujli_unioned AS ujli
        LEFT JOIN exchange_rates AS er_month
            ON ujli.company_code_currency = er_month.from_currency
            AND ujli.posting_date_last_day = er_month.valid_from
        LEFT JOIN exchange_rates AS er_year
            ON ujli.company_code_currency = er_year.from_currency
            AND ujli.posting_date_start_of_year = er_year.valid_from
        LEFT JOIN decimal_places_in_currencies AS dp 
            ON ujli.company_code_currency = dp.currency_key
        LEFT JOIN company_code AS cc
            ON ujli.company_code = cc.company_code
''')

ujli_fx.createOrReplaceTempView('ujli_fx')

# COMMAND ----------

ujli_converted = spark.sql(f'''
    SELECT
        -- keys
        fftd.*
        ,fftd.posting_date AS date_key
        ,CAST(fftd.amount_in_company_code_currency AS DECIMAL(18,4)) AS amount_local_currency
        ,fftd.company_code_currency AS local_currency_code
        ,CAST(
            CASE
                WHEN fftd.actual_plan_code <> 'Actual' 
                    THEN fftd.amount_in_company_code_currency * fftd.fx_rate_planning
                WHEN fftd.balance_sheet_account_flag = True
                    THEN fftd.amount_in_company_code_currency * fftd.fx_rate_month_end
                -- TODO: scaling should be done only in one place
                WHEN fftd.datasource IN ('SGR', 'SGRJNLS')
                    THEN fftd.amount_in_global_currency
                ELSE fftd.amount_in_company_code_currency * fftd.fx_rate_avg_monthly
            END AS DECIMAL(18,4)
        ) AS amount_group_currency
        ,CAST(
            fftd.amount_in_company_code_currency
            * fftd.fx_rate_month_end
            AS DECIMAL(18,4)
        ) AS amount_group_currency_month_end
        ,CAST(
            fftd.amount_in_company_code_currency
            * fftd.fx_rate_planning
            AS DECIMAL(18,4)
         ) AS amount_group_currency_plan_rate
    FROM
        ujli_fx AS fftd
    WHERE
        -- exclude ghost records 
        fftd.gl_account <> '' 
        AND fftd.chart_of_accounts <> ''
''')

ujli_converted.createOrReplaceTempView('ujli_converted')

# COMMAND ----------

ujli_elimination_flag = spark.sql(f'''
    SELECT ujlic.*
        EXCEPT(ujlic.elimination_flag),
        CASE
            WHEN ujlic.elimination_flag = '' THEN 'R'
            ELSE ujlic.elimination_flag
        END AS elimination_flag
    FROM ujli_converted AS ujlic                           
''')

ujli_elimination_flag.createOrReplaceTempView('ujli_elimination_flag')

# COMMAND ----------

gold_table = spark.sql(f'''
    SELECT
        -- keys
        ujlic.date_key
        ,ujlic.country_key
        ,ujlic.profit_center 
        ,ujlic.ifrs_flag
        ,ujlic.controlling_area
        ,ujlic.line_of_business
        ,ujlic.line_of_business_1
        ,ujlic.cost_center
        ,ujlic.company_code
        ,ujlic.gl_account
        ,ujlic.chart_of_accounts
        ,ujlic.datasource
        ,ujlic.vcode
        -- TODO: currency dim doesn't exist yet 
        ,0 AS local_currency_skey
        ,0 AS group_currency_skey
        ,ujlic.elimination_flag
        ,ujlic.actual_plan_code

        -- Calculate the measures
        ,ujlic.amount_local_currency
        ,ujlic.local_currency_code
        ,ujlic.amount_group_currency
        ,ujlic.amount_group_currency_month_end
        ,ujlic.amount_group_currency_plan_rate
        ,0 AS amount_group_currency_plus_1_year_rate
        ,ujlic.volume_kg
        ,ujlic.volume_litres_l20
        ,ujlic.volume_issued_litres_l20
        ,ujlic.vcode_amount_local
        ,CASE
            WHEN ujlic.vcode NOT IN ('V.001', 'V.002')
                THEN ujlic.amount_group_currency
            ELSE ujlic.vcode_amount_group
        END AS vcode_amount_group
        -- reconiliation columns
        ,ujlic.document_number
        ,ujlic.posting_item
        ,ujlic.document_status
        ,ujlic.document_type
        ,ujlic.customer
        ,ujlic.bill_to_party
        ,ujlic.ship_to_party
        ,ujlic.material

        -- (rates)
        ,ujlic.fx_rate_avg_monthly
        ,ujlic.fx_rate_month_end
        ,ujlic.fx_rate_planning
        ,ujlic.balance_sheet_account_flag
    FROM
        ujli_elimination_flag AS ujlic
''')

gold_table.createOrReplaceTempView('gold_table')

# COMMAND ----------

hashed_gold_table = spark.sql(
    f'''
    SELECT
        {metadata.get_fkey_ddl(["g.profit_center", "g.controlling_area"])} AS profit_center_skey
        ,{metadata.get_fkey_ddl(["g.line_of_business_1"])}                 AS line_of_business_skey
        ,{metadata.get_fkey_ddl(["g.company_code"])}                       AS company_code_skey
        ,{metadata.get_fkey_ddl(["g.vcode"])}                              AS vcode_skey
        ,{metadata.get_fkey_ddl(["g.chart_of_accounts", "g.cost_center"])} AS cost_center_skey
        ,{metadata.get_fkey_ddl(["g.chart_of_accounts", "g.gl_account"])}  AS gl_account_skey
        ,{metadata.get_fkey_ddl(["g.datasource"])}                         AS datasource_skey
        ,{metadata.get_fkey_ddl(["g.material"])}                           AS material_skey
        ,g.*
    FROM
        gold_table AS g
    '''
)

hashed_gold_table.createOrReplaceTempView('hashed_gold_table')

# COMMAND ----------

etl_fields = spark.sql(f'''
    {metadata.get_etl_fields_ddl('hashed_gold_table')}
''')

etl_fields.createOrReplaceTempView('etl_fields')

# COMMAND ----------

metadata.insert_overwrite('etl_fields', destination)