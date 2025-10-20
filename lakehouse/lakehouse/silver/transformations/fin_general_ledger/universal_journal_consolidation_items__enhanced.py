# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = 'silver.universal_journal_consolidation_items.yaml'
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')

# COMMAND ----------

metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

universal_journal_consolidation_items__casted_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("universal_journal_consolidation_items__casted", True)}'

material_master_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("material_master", True)}'

sgr_manual_adjustments_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("sgr_manual_adjustments", True)}'

fs_item_mapping_to_special_mapping_version_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("fs_item_mapping_to_special_mapping_version", True)}'

fs_item_mapping_to_gl_account_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("fs_item_mapping_to_gl_account", True)}'

sgr_cons_item_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("sgr_cons_item", True)}'

faccc_mapping_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("faccc_mapping", True)}'

decimal_places_in_currencies_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("decimal_places_in_currencies", True)}'

cost_center_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("cost_center", True)}'

profit_center_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("profit_center", True)}'

exchange_rates_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("exchange_rates", True)}'

vcode_gl_account_mapping_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("vcode_gl_account_mapping", True)}'

company_code_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("company_code", True)}'

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", True)}'

# COMMAND ----------

material_md = spark.sql(
    f'''
        SELECT
            mat.material_number,
            mat.material_group,
            mat.ranking 
        FROM {material_master_tablename} AS mat
        WHERE ranking = 1
    '''
)
 
material_md.createOrReplaceTempView('material_md')

# COMMAND ----------

ujci_hop1 = spark.sql(
    f'''
        SELECT
            -- keys
            ujci.client,
            ujci.document_number,
            ujci.posting_item,
            ujci.fiscal_year,
            ujci.dimension,
            ujci.ledger,

            -- payload
            ujci.original_compcode,
            ujci.company,
            ujci.controlling_area,
            ujci.record_type,
            ujci.consolidation_version,
            ujci.transaction_currency,
            ujci.local_currency,

            CASE
                WHEN ujci.posting_period = 13
                    THEN 12
               ELSE ujci.posting_period
            END AS posting_period,

            ujci.document_category,
            ujci.consolidation_unit,
            ujci.financial_statement_item,
            ujci.subitem_category,
            ujci.subitem,
            ujci.posting_level,
            ujci.document_type,
            ujci.ledger_currency,
            ujci.base_unit_of_measure,
            ujci.gl_account,

            CASE 
                WHEN 
                    ujci.material_number IS NULL 
                        OR 
                    LENGTH(ujci.material_number) <= 1
                    THEN mat.material_number
                ELSE ujci.material_number
            END AS material_number,

            CASE 
                WHEN substring(ujci.fiscal_year_period, 5, 3) = '013' 
                    THEN concat(left(ujci.fiscal_year_period, 4), '012') 
                ELSE ujci.fiscal_year_period 
            END AS fiscal_year_period,
            
            ujci.cost_center,
            ujci.profit_center,
            ujci.date_created,
            ujci.chart_of_accounts,
            ujci.consolidation_chart_of_accounts,
            ujci.ct_flag,
            ujci.ship_to_party,
            ujci.bill_to_party,
            ujci.customer_group,
            ujci.product_sold,
            ujci.product_sold_group,
            ujci.division,
            ujci.partner_unit,
            ujci.distribution_channel,
            ujci.sales_organization,
            ujci.customer,
            ujci.supplier,
            ujci.plant,
            ujci.posting_date,
            ujci.segment,
            ujci.trading_partner_no,
            'SGR' AS datasource,

            CASE
                WHEN 
                    ujci.document_type LIKE '2%'
                        AND
                    left(ujci.consolidation_unit, 5) = 'ICZA0' 
                        AND
                    left(ujci.partner_unit, 5) = 'ICZA1'
                    THEN 'L'
                WHEN 
                    ujci.document_type LIKE '2%'
                        AND
                    left(ujci.consolidation_unit, 5) = 'ICZA1' 
                        AND
                    left(ujci.partner_unit, 5) = 'ICZA0'
                    THEN 'L'
                WHEN 
                    ujci.document_type LIKE '2%'
                        AND
                    left(ujci.consolidation_unit, 6) = 'ICMA01' 
                        AND
                    left(ujci.partner_unit, 6) = 'ICMA04'
                    THEN 'G'
                WHEN 
                    ujci.document_type LIKE '2%'
                        AND
                    left(ujci.consolidation_unit, 6) = 'ICMA04' 
                        AND
                    left(ujci.partner_unit, 6) = 'ICMA01'
                    THEN 'G'
                WHEN 
                    ujci.document_type LIKE '2%'
                        AND
                    left(ujci.consolidation_unit, 5) <> left(ujci.partner_unit, 5)
                    THEN 'G'
                WHEN 
                    ujci.document_type LIKE '2%'
                        AND
                    left(ujci.consolidation_unit, 5) = left(ujci.partner_unit, 5)
                    THEN 'L'
                ELSE ''
            END AS elimination_flag,

            ujci.amount_in_trans_currency,
            ujci.quantity,
            ujci.amount_in_group_currency,
            ujci.amount_in_local_currency,
            ujci.time_created,
            -- metadata
            ujci.__etl_keys_fprint,
            ujci.__etl_effective_from,
            ujci.__etl_effective_to,
            ujci.__etl_is_active,
            ujci.__etl_is_deleted
        FROM
        {universal_journal_consolidation_items__casted_tablename} AS ujci
        LEFT JOIN material_md AS mat
            ON ujci.product_sold_group = mat.material_group
    '''
)
 
ujci_hop1.createOrReplaceTempView('ujci_hop1')

# COMMAND ----------

ujci_agg_sgr_man = spark.sql(f'''
   SELECT
        sgrma.fiscal_year,
        sgrma.dimension,
        sgrma.ledger,
        sgrma.controlling_area,
        sgrma.record_type,
        sgrma.consolidation_version,
        sgrma.local_currency,
        sgrma.posting_period,
        sgrma.document_category,
        sgrma.consolidation_unit,
        sgrma.financial_statement_item,
        sgrma.subitem_category,
        sgrma.subitem,
        sgrma.posting_level,
        sgrma.document_type,
        sgrma.base_unit_of_measure,
        sgrma.gl_account, 
        sgrma.material_number,
        sgrma.cost_center,
        sgrma.profit_center,
        sgrma.date_created,
        sgrma.chart_of_accounts,
        sgrma.consolidation_chart_of_accounts,
        sgrma.quantity,
        sum(sgrma.amount_in_local_currency) AS amount_in_local_currency,
        sgrma.__etl_keys_fprint,
        sgrma.__etl_effective_from,
        sgrma.__etl_effective_to,
        sgrma.__etl_is_active,
        sgrma.__etl_is_deleted
    FROM {sgr_manual_adjustments_tablename} AS sgrma
    GROUP BY
        ALL; 
''')

ujci_agg_sgr_man.createOrReplaceTempView('ujci_agg_sgr_man')

# COMMAND ----------

ujci_hop2 = spark.sql(
    f'''
        SELECT 
            -- keys
            '' AS client,
            '' AS document_number,
            '' AS posting_item,
            sgrma.fiscal_year,
            sgrma.dimension,
            sgrma.ledger,

            -- payload
            '' AS original_compcode,
            '' AS company,
            sgrma.controlling_area,
            sgrma.record_type,
            sgrma.consolidation_version,
            '' AS transaction_currency,
            sgrma.local_currency,
            sgrma.posting_period,
            sgrma.document_category,
            sgrma.consolidation_unit,
            sgrma.financial_statement_item,
            sgrma.subitem_category,
            sgrma.subitem,
            sgrma.posting_level,
            sgrma.document_type,
            'USD' AS ledger_currency,
            sgrma.base_unit_of_measure,

            CASE 
                WHEN length(sgrma.gl_account) = 9 
                    THEN CONCAT('0', sgrma.gl_account)
                WHEN length(sgrma.gl_account) = 8 
                    THEN CONCAT('00', sgrma.gl_account)
                WHEN length(sgrma.gl_account) = 7 
                    THEN CONCAT('000', sgrma.gl_account) 
                ELSE sgrma.gl_account 
            END AS gl_account,
            sgrma.material_number,

            concat(
                cast(sgrma.fiscal_year AS STRING), 
                lpad(
                    cast(sgrma.posting_period AS STRING), 3, '0'
                    )
                ) AS fiscal_year_period,
                
            sgrma.cost_center,
            sgrma.profit_center,
            sgrma.date_created,
            sgrma.chart_of_accounts,
            sgrma.consolidation_chart_of_accounts,
            0 AS ct_flag,
            '' AS ship_to_party,
            '' AS bill_to_party,
            '' AS customer_group,
            '' AS product_sold,
            '' AS product_sold_group,
            '' AS division,
            '' AS partner_unit,
            '' AS distribution_channel,
            '' AS sales_organization,
            '' AS customer,
            '' AS supplier,
            '' AS plant,
            '1900-01-01' AS posting_date,
            '' AS segment,
            '' AS trading_partner_no,
            'SGRJNLS' AS datasource,
            '' AS elimination_flag,
            0 AS amount_in_trans_currency,
            sgrma.quantity,
            CAST(0.0 AS DECIMAL(28, 8)) AS amount_in_group_currency,
            sgrma.amount_in_local_currency,
            '1900-01-01 00:00:00' AS time_created,
            sgrma.__etl_keys_fprint,
            sgrma.__etl_effective_from,
            sgrma.__etl_effective_to,
            sgrma.__etl_is_active,
            sgrma.__etl_is_deleted
        FROM ujci_agg_sgr_man AS sgrma
    '''
)
 
ujci_hop2.createOrReplaceTempView('ujci_hop2')

# COMMAND ----------

ujci_unioned = spark.sql(
    f'''
        SELECT * FROM ujci_hop1
            UNION ALL
        SELECT * FROM ujci_hop2
    '''
)

ujci_unioned.createOrReplaceTempView('ujci_unioned')

# COMMAND ----------

ujci_hop3 = spark.sql(
    f'''
        SELECT
            ujci.*,
            
            CASE 
                WHEN ujci.posting_period = 0
                    THEN LAST_DAY(DATE_ADD(MAKE_DATE(ujci.fiscal_year -1, 12, 1), 0))
                ELSE LAST_DAY(MAKE_DATE(ujci.fiscal_year, ujci.posting_period,1))
            END AS month_end_date

        FROM ujci_unioned AS ujci
    '''
)
 
ujci_hop3.createOrReplaceTempView('ujci_hop3')

# COMMAND ----------

ujci_hop4 = spark.sql(
    f'''
        SELECT
            ujci.*,
            fsimap.fs_item_mapping_id,
            fsimap.fs_item_mapping_version,
            sci.company_code,
            CASE 
                WHEN fm.cost_center_functional_area_category IS NULL
                    THEN '9'
                ELSE fm.cost_center_functional_area_category
            END AS cost_center_functional_area_category,
            cc.cost_center_category,
            fsimapitm.first_gl_account,
            dp.currency_decimal_places,
            prctr.volume_flag_ind,
            prctr.line_of_business_1
        FROM ujci_hop3 AS ujci
        INNER JOIN 
            {fs_item_mapping_to_special_mapping_version_tablename} AS fsimap
            ON
                ujci.chart_of_accounts = fsimap.chart_of_accounts
                    AND
                ujci.consolidation_version = fsimap.fs_item_mapping_version
                    AND
                ujci.consolidation_chart_of_accounts = fsimap.consolidation_coa
                    AND
                ujci.month_end_date BETWEEN fsimap.valid_from AND fsimap.valid_to
        LEFT JOIN 
            {sgr_cons_item_tablename} AS sci
            ON
                ujci.consolidation_unit = sci.consolidation_unit
        LEFT JOIN 
            {faccc_mapping_tablename} AS fm
            ON
                ujci.subitem = fm.functional_area
        LEFT JOIN
            {cost_center_tablename} AS cc
            ON
                ujci.cost_center = cc.cost_center
                    AND
                cc.valid_to = '9999-12-31'
        LEFT JOIN
            {fs_item_mapping_to_gl_account_tablename} AS fsimapitm
            ON
                ujci.consolidation_chart_of_accounts = fsimapitm.consolidation_coa
                    AND
                ujci.chart_of_accounts = fsimapitm.chart_of_accounts
                    AND
                ujci.gl_account = fsimapitm.account_number
                    AND
                ujci.financial_statement_item = fsimapitm.fs_item
                    AND
                fsimap.fs_item_mapping_id = fsimapitm.fs_item_mapping_id
                    AND
                fsimap.fs_item_mapping_revision = fsimapitm.fs_item_mapping_revision
        LEFT JOIN 
            {decimal_places_in_currencies_tablename} AS dp
            ON
                ujci.local_currency = dp.currency_key
        LEFT JOIN
            {profit_center_tablename} AS prctr
            ON
                ujci.profit_center = prctr.profit_center
    '''
)
 
ujci_hop4.createOrReplaceTempView('ujci_hop4')

# COMMAND ----------

ujci_hop5 = spark.sql(
    f'''
        SELECT 
        -- keys
            ujci.client,
            ujci.document_number,
            ujci.posting_item,
            ujci.fiscal_year,
            ujci.dimension,
            ujci.ledger,

          -- payload
            ujci.original_compcode,
            ujci.company,
            ujci.controlling_area,
            ujci.record_type,
            ujci.consolidation_version,
            ujci.transaction_currency,
            ujci.local_currency,
            ujci.posting_period,
            ujci.document_category,

            ujci.consolidation_unit,

            ujci.financial_statement_item,
            ujci.subitem_category,
            ujci.subitem,
            ujci.posting_level,
            ujci.document_type,
            ujci.ledger_currency,
            ujci.base_unit_of_measure,

            CASE 
                WHEN 
                    ujci.gl_account IS NULL
                        OR
                    ujci.gl_account = ''
                    THEN ujci.first_gl_account
                ELSE ujci.gl_account
            END AS gl_account,

            ujci.material_number,
            ujci.fiscal_year_period,
            ujci.cost_center,
            ujci.profit_center,
            ujci.date_created,
            ujci.chart_of_accounts,
            ujci.consolidation_chart_of_accounts,
            ujci.ct_flag,
            ujci.ship_to_party,
            ujci.bill_to_party,
            ujci.customer_group,
            ujci.product_sold,
            ujci.product_sold_group,
            ujci.division,

            CASE 
                WHEN ujci.elimination_flag IS NULL 
                        OR
                    ujci.elimination_flag = ''
                    THEN ''
                ELSE ujci.partner_unit
            END AS partner_unit,

            ujci.distribution_channel,
            ujci.sales_organization,
            ujci.customer,
            ujci.supplier,
            ujci.plant,
            ujci.posting_date,
            ujci.segment,
            ujci.trading_partner_no,
            ujci.datasource,
            ujci.elimination_flag,
            ujci.amount_in_trans_currency,
            ujci.quantity,
            ujci.amount_in_group_currency,
            ujci.amount_in_local_currency,
            ujci.fs_item_mapping_id,
            ujci.fs_item_mapping_version,

            CASE
                WHEN 
                    ujci.amount_in_local_currency = 0 
                        AND
                    ujci.amount_in_group_currency != 0
                        AND
                    ujci.document_type IN ('1A','2E','2F','2G','2H','2J','2K','2L','2M')
                        AND
                    ujci.ct_flag != '4'
                    THEN 'Y'
                ELSE 'N'
            END AS gc2lc_flag,

            CASE
                WHEN ujci.company_code IS NULL    
                    THEN right(ujci.consolidation_unit, 4)
                ELSE ujci.company_code
            END AS company_code,


            ujci.cost_center_functional_area_category,
            ujci.cost_center_category,
            ujci.first_gl_account,

            CASE
                WHEN ujci.currency_decimal_places IS NULL
                    THEN 2
                ELSE ujci.currency_decimal_places
            END AS currency_decimal_places,

            CASE
                WHEN ujci.cost_center_category IS NOT NULL
                    THEN CONCAT(CONCAT(ujci.gl_account, '_'), ujci.cost_center_category)
                WHEN ujci.gl_account <= '0500000000'
                    THEN CONCAT(CONCAT(ujci.gl_account, '_'), ujci.cost_center_functional_area_category)
                ELSE CONCAT(CONCAT(ujci.gl_account, '_'), '9')
            END AS vcode_join_field,

            ujci.time_created,
            ujci.month_end_date,
            ujci.volume_flag_ind,
            ujci.line_of_business_1,
            ujci.__etl_keys_fprint,
            ujci.__etl_effective_from,
            ujci.__etl_effective_to,
            ujci.__etl_is_active,
            ujci.__etl_is_deleted
        FROM ujci_hop4 AS ujci
    '''
)
 
ujci_hop5.createOrReplaceTempView('ujci_hop5')

# COMMAND ----------

ujci_hop6 = spark.sql(
    f'''
        SELECT
            ujci.*,
            vcode_map.vcode,
            ex.scaled_exchange_rate,
            compc.entity_grouping_level_top,

            cast(
                ujci.amount_in_group_currency * (1 / ex.scaled_exchange_rate)
                AS DECIMAL(28, 8)
            )
            AS amount_gc2lc,

            cast(
                ujci.amount_in_local_currency * (ex.scaled_exchange_rate)
                AS DECIMAL(28, 8)
            )
            AS amount_lc2gc

        FROM ujci_hop5 AS ujci
        LEFT JOIN
            {exchange_rates_tablename} as ex
            ON
                ujci.local_currency = ex.from_currency
                    AND
                ujci.ledger_currency = ex.to_currency
                    AND
                ujci.month_end_date = ex.valid_from
                    AND
                ex.exchange_rate_type = 'I'
        LEFT JOIN
            {vcode_gl_account_mapping_tablename} AS vcode_map
            ON
                ujci.vcode_join_field = vcode_map.vcode_join_field
        LEFT JOIN
            {company_code_tablename} AS compc
            ON
                ujci.company_code = compc.company_code
    '''
)
 
ujci_hop6.createOrReplaceTempView('ujci_hop6')

# COMMAND ----------

ujci_hop7 = spark.sql(
    f'''
        SELECT
        -- keys
            ujci.client,
            ujci.document_number,
            ujci.posting_item,
            ujci.fiscal_year,
            ujci.dimension,
            ujci.ledger,

        -- payload
            ujci.original_compcode,
            ujci.company,
            ujci.controlling_area,
            ujci.record_type,
            ujci.consolidation_version,
            ujci.transaction_currency,
            ujci.local_currency,
            ujci.posting_period,
            ujci.document_category,
            ujci.consolidation_unit,
            ujci.financial_statement_item,
            ujci.subitem_category,
            ujci.subitem,
            ujci.posting_level,
            ujci.document_type,
            ujci.ledger_currency,
            ujci.base_unit_of_measure,
            ujci.gl_account,
            ujci.material_number,
            ujci.fiscal_year_period,
            ujci.cost_center,
            ujci.profit_center,
            ujci.date_created,
            ujci.chart_of_accounts,
            ujci.consolidation_chart_of_accounts,
            ujci.ct_flag,
            ujci.ship_to_party,
            ujci.bill_to_party,
            ujci.customer_group,
            ujci.product_sold,
            ujci.product_sold_group,
            ujci.division,
            ujci.partner_unit,
            ujci.distribution_channel,
            ujci.sales_organization,
            ujci.customer,
            ujci.supplier,
            ujci.plant,
            ujci.posting_date,
            ujci.segment,
            ujci.trading_partner_no,
            ujci.datasource,
            ujci.elimination_flag,
            ujci.fs_item_mapping_id,
            ujci.fs_item_mapping_version,
            ujci.gc2lc_flag,

            CASE 
                WHEN trim(ujci.entity_grouping_level_top) IN ('Vivo Energy', 'ENGEN') 
                    AND ujci.elimination_flag = 'G' 
                    THEN 'CONS'
                WHEN trim(ujci.entity_grouping_level_top) = 'SVL' 
                    AND ujci.elimination_flag IN ('G','L') 
                    THEN 'SCON'
                ELSE ujci.company_code
            END AS company_code,
            
            ujci.cost_center_category,
            ujci.currency_decimal_places,
            ujci.vcode,
            ujci.amount_in_trans_currency,

            CASE 
                WHEN ujci.gl_account IN ('0100000010','0100000021') 
                        AND 
                    ujci.volume_flag_ind = 'Y' 
                    THEN ujci.quantity * (-1) 
                ELSE 0
            END AS quantity,
            
            CASE
                WHEN ujci.gc2lc_flag = 'Y'
                    THEN ujci.amount_gc2lc * (-1)
                WHEN ujci.datasource = 'SGRJNLS'
                    THEN ujci.amount_in_local_currency * (-1)
                ELSE cast(ujci.amount_in_local_currency / POWER(10, cast(ujci.currency_decimal_places AS INT) - 2) AS DECIMAL(28, 8)) * (-1)
            END AS amount_in_local_currency,

            CASE
                WHEN ujci.gc2lc_flag = 'Y'
                    THEN amount_in_group_currency * (-1)
                WHEN ujci.datasource = 'SGRJNLS'
                    THEN ujci.amount_lc2gc * (-1)
                ELSE ujci.amount_lc2gc / POWER(10, cast(ujci.currency_decimal_places AS INT) - 2) * (-1)
            END AS amount_in_group_currency,
            
            ujci.time_created,
            ujci.volume_flag_ind,
            ujci.line_of_business_1,
            ujci.entity_grouping_level_top,
            ujci.month_end_date,
            ujci.__etl_keys_fprint,
            ujci.__etl_effective_from,
            ujci.__etl_effective_to,
            ujci.__etl_is_active,
            ujci.__etl_is_deleted
    
        FROM ujci_hop6 AS ujci
    '''
)
 
ujci_hop7.createOrReplaceTempView('ujci_hop7')

# COMMAND ----------

ujci_select = spark.sql(
    f'''
        SELECT 
        -- keys
            ujci.client,
            ujci.document_number,
            ujci.posting_item,
            ujci.fiscal_year,
            ujci.dimension,
            ujci.ledger,

        -- payload
            ujci.original_compcode,
            ujci.company,
            ujci.controlling_area,
            ujci.record_type,
            ujci.consolidation_version,
            ujci.transaction_currency,
            ujci.local_currency,
            ujci.posting_period,
            ujci.document_category,
            ujci.consolidation_unit,
            ujci.financial_statement_item,
            ujci.subitem_category,
            ujci.subitem,
            ujci.posting_level,
            ujci.document_type,
            ujci.ledger_currency,
            ujci.base_unit_of_measure,
            ifnull(ujci.gl_account, '') as gl_account,
            ujci.material_number,
            ujci.fiscal_year_period,
            ujci.cost_center,
            ujci.profit_center,
            ujci.date_created,
            ujci.chart_of_accounts,
            ujci.consolidation_chart_of_accounts,
            ujci.ct_flag,
            ujci.ship_to_party,
            ujci.bill_to_party,
            ujci.customer_group,
            ujci.product_sold,
            ujci.product_sold_group,
            ujci.division,
            ujci.partner_unit,
            ujci.distribution_channel,
            ujci.sales_organization,
            ujci.customer,
            ujci.supplier,
            ujci.plant,
            ujci.posting_date,
            ujci.segment,
            ujci.trading_partner_no,
            ujci.datasource,
            ujci.elimination_flag,
            ujci.fs_item_mapping_id,
            ujci.fs_item_mapping_version,
            ujci.gc2lc_flag,
            ujci.company_code,
            ujci.cost_center_category,
            ujci.currency_decimal_places,
            ifnull(ujci.vcode, '') as vcode,
            ujci.amount_in_trans_currency,
            ifnull(ujci.quantity, 0) AS quantity,
            ifnull(ujci.amount_in_group_currency, 0) AS amount_in_group_currency,
            ifnull(ujci.amount_in_local_currency, 0) as amount_in_local_currency,
            ujci.time_created,
            ujci.entity_grouping_level_top,
            ujci.month_end_date
        FROM ujci_hop7 AS ujci
    '''
)
 
ujci_select.createOrReplaceTempView('ujci_select')

# COMMAND ----------

write_result = metadata.process_transformation_table('ujci_select', env_vars.silver_catalog)
logger.log.info(f'Write: {dest_tablename} {write_result}')