# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

# MAGIC %run ../../../common/silver_metadata

# COMMAND ----------

logger.log.info(f'Execution started for {runtime_context.notebook_name}')

# COMMAND ----------

metadata_filename = 'silver.universal_journal_line_items.yaml'
logger.log.info(f'Widget: "metadata_filename" = {metadata_filename}')
metadata = MetadataYaml(f'./metadata/{metadata_filename}')

# COMMAND ----------

universal_journal_line_items_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("universal_journal_line_items", True)}'

universal_journal_consolidation_items_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("universal_journal_consolidation_items", True)}'

finance_transactions_consolidation_historic_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("finance_transactions_consolidation_historic", True)}'

vcode_gl_account_mapping_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("vcode_gl_account_mapping", True)}'

cost_center_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("cost_center", True)}'

profit_center_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("profit_center", True)}'

gl_account_master_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("gl_account_master", True)}'

compcode_tablename = f'{env_vars.silver_catalog}.{metadata.sources_2partname("flat_compcode", True)}'

dest_tablename = f'{env_vars.silver_catalog}.{metadata.destination_2partname("silver", True)}'

# TODO: move the join logic back into the Gold layer
# This Silver notebook should only be working with the universal_journal_line_items table

# COMMAND ----------

finance_transactions_consolidation_historic = spark.sql(
    f'''
        SELECT
            ftch.* EXCEPT(ftch.profit_center)
            ,ftch.company_code || ftch.profit_center AS profit_center
            ,CASE
                WHEN ftch.consolidation_record_type = 'Rep_Cost' THEN True
                ELSE False
            END AS ifrs_flag
        FROM
            {finance_transactions_consolidation_historic_tablename} AS ftch
        WHERE
            ftch.quantity <> 0
            OR ftch.volume_kg <> 0 
            OR ftch.volume_litres_l20 <> 0
            OR ftch.vcode_amount_local <> 0
    '''
)

finance_transactions_consolidation_historic.createOrReplaceTempView('finance_transactions_consolidation_historic')

# COMMAND ----------

universal_journal_consolidation_items = spark.sql(f'''
    WITH joined_exclusion AS (
        SELECT
            ujci.* EXCEPT(ujci.month_end_date, ujci.posting_date)
            ,ujci.month_end_date AS posting_date
            ,CASE
                WHEN 
                    (cc.entity_grouping_level_top = 'ENGEN' 
                    OR ujci.company_code IN ('NA02', 'CD01')) 
                    AND ujci.fiscal_year_period >= 2024001 
                THEN 'N'
                WHEN ujci.fiscal_year_period <= 2024009 
                THEN 'Y'
                ELSE 'N'
            END as exclusion_flag
        FROM
            {universal_journal_consolidation_items_tablename} AS ujci
            LEFT JOIN {compcode_tablename} AS cc
                ON ujci.company_code = cc.company_code
    )
    SELECT
        *
    FROM
        joined_exclusion
    WHERE
        exclusion_flag = 'N'
        AND (quantity <> 0
        OR amount_in_group_currency <> 0
        OR amount_in_local_currency <> 0)
''')

universal_journal_consolidation_items.createOrReplaceTempView('universal_journal_consolidation_items')

# COMMAND ----------

vcode_mapping = spark.sql(f'''
        SELECT DISTINCT
            vc.gl_account || '_' || vc.cost_center_category AS join_code
            ,vc.vcode
        FROM
            {vcode_gl_account_mapping_tablename} AS vc
        WHERE
            vc.__etl_is_active = True
    UNION ALL
        SELECT DISTINCT
            vc.gl_account
            ,vc.vcode
        FROM
            {vcode_gl_account_mapping_tablename} AS vc
        WHERE
            vc.__etl_is_active = True
            AND vc.cost_center_category = '9'
''')

vcode_mapping.createOrReplaceTempView('vcode_mapping')

# COMMAND ----------

gl_account_master = spark.sql(f'''
    SELECT
        glam.gl_account
        ,glam.gl_account_type
        ,glam.balance_sheet_account_flag
    FROM
        {gl_account_master_tablename} AS glam
    WHERE
        glam.chart_of_accounts = 'OP01'
        AND glam.__etl_is_active = True
''')

gl_account_master.createOrReplaceTempView('gl_account_master')

# COMMAND ----------

profit_center = spark.sql(f'''
    SELECT
        pc.profit_center
        ,pc.line_of_business
        ,pc.line_of_business_1
        ,pc.volume_flag_ind
    FROM
        {profit_center_tablename} AS pc
    WHERE
        pc.__etl_is_active = 'Y'
''')

profit_center.createOrReplaceTempView('profit_center')

# COMMAND ----------

cost_center = spark.sql(f'''
    SELECT
        cc.cost_center
        ,cc.cost_center_category
        ,LAG(cc.valid_to, 1, '1999-01-01') OVER (PARTITION BY cc.cost_center ORDER BY cc.valid_to ASC) AS valid_from
        ,cc.valid_to
    FROM
        {cost_center_tablename} AS cc
    WHERE
        cc.controlling_area = 'OP01'
        AND cc.__etl_is_active = True
''')

cost_center.createOrReplaceTempView('cost_center')

# COMMAND ----------

universal_journal_line_items_filtered = spark.sql(f'''
    WITH exclusion_flags AS (
        SELECT
            ujli.*,
            CASE
                WHEN ujli.company_code = 'CD01' 
                    AND ujli.fiscal_year_period <= 2025003 
                THEN 'Y'
                WHEN (cc.entity_grouping_level_top = 'ENGEN' OR ujli.company_code = 'NA02') 
                    AND ujli.fiscal_year_period <= 2024009 
                THEN 'Y'
                WHEN ujli.ledger = 'A1' 
                    AND ujli.fiscal_year_period >= 2024010 
                THEN 'Y'
                ELSE 'N'
            END AS exclusion_flag
        FROM
            {universal_journal_line_items_tablename} AS ujli
            LEFT JOIN {compcode_tablename} AS cc
                ON ujli.company_code = cc.company_code
    )
    SELECT
        ujli.* EXCEPT (
            amount_in_company_code_currency,
            additional_quantity_1,
            additional_quantity_2,
            quantity
        ),
        ujli.amount_in_company_code_currency * -1  AS amount_in_company_code_currency,
        ujli.additional_quantity_1 * -1            AS additional_quantity_1,
        ujli.additional_quantity_2 * -1            AS additional_quantity_2,
        ujli.quantity * -1                         AS quantity
    FROM
        exclusion_flags AS ujli
    WHERE
        (ujli.amount_in_company_code_currency <> 0
        OR ujli.amount_in_global_currency <> 0
        OR ujli.additional_quantity_1 <> 0
        OR ujli.additional_quantity_2 <> 0)
        AND ujli.fiscal_year_variant = 'K4'
        AND ujli.controlling_area = 'OP01'
        AND (
            (ujli.fiscal_year_period <= 2024009 AND ujli.ledger = 'A1')
            OR ujli.ledger = '0L'
        )
        AND
        ujli.exclusion_flag = 'N'

    /*
            From Ricardo on 15-Apr-2025
            
            [1] when Source = 'ACTUAL' and DataSource like 'SGR%' 
                    and ("Entity Grouping level Top" = 'ENGEN' or Company_Code in ('NA02', 'CD01')) and YearPeriod >= '2024001'
                then 'N'
            [2] when Source = 'ACTUAL' and DataSource like 'S/4HANA'
                    and Company_Code = 'CD01' and YearPeriod <= '2025003'
                then 'Y'     
            [3] when Source = 'ACTUAL' and DataSource like 'S/4HANA'
                    and ("Entity Grouping level Top" = 'ENGEN' or Company_Code = 'NA02') and YearPeriod <= '2024009'
                then 'Y' 
            [4] when Source = 'ACTUAL' and DataSource like 'S/4HANA' and RLDNR = 'A1' and YearPeriod >= '2024010'
                then 'Y' 
            [5] when Source = 'ACTUAL' and DataSource like 'SGR%' and YearPeriod <= '2024009'
                then 'Y'
 
            The context of these exclusion is that the basis of the finance model data [5] is sourced as follows:
            - SAP S4/Hanna for all period
            - HFM adjustments (incl. consolidation entries) up to Sep-24, and
            - SGR (ACDOCU) from Oct-24 onwards
 
            The exclusions are then overlayed as follows:
            1. For ENGEN Countries (as defined in the company code hierarchy), CD01 and NA02,
                the data will be from Jan-24 as these are in SGR only and not in SAP S4/Hanna
            2. For CD01 (DRC):  Go-live date in SAP S4Hana is Apr-25, data to be sourced from  SAP S4/Hanna is Apr-25 onwards.
                This is to filter out the take-on balance for CD01 in SAP S4/Hanna with a posting date of 31 Mar 2025
                (already reported in SGR)
            3. For ENGEN Countrie:  Go-live date in SAP S4Hana is Oct-24, data to be sourced from  SAP S4/Hanna is Oct-24 onwards.
                This is to filter out the take-on balance for these countries in SAP S4/Hanna with a posting date of 30 Sep 2024 (already reported in SGR)
            4. With SGR, the intercompany eliminations are done with the SGR model.
                Ledger Type “A1” where elimination entries were booked for “local” entities (for example Tunisia, Cape Verde)
                for HFM reporting is no longer considered for SGR.
                Hence, if the period is feeding from HFM, Ledger Type “A1” is included, otherwise it should be excluded.
        */
''')

universal_journal_line_items_filtered.createOrReplaceTempView('universal_journal_line_items_filtered')

# COMMAND ----------

combined_universal_journal_line_items = spark.sql(f'''
        SELECT
            'ujli' AS _indicator
            -- Keys
            ,'Actual' AS actual_plan_code
            ,'S4HANA' AS datasource
            ,'' AS journal_type
            ,ujli.posting_date
            ,ujli.account_number AS gl_account
            ,ujli.chart_of_accounts
            ,ujli.account_type
            ,ujli.offsetting_account_number
            ,ujli.fiscal_year_period
            ,ujli.fiscal_year
            ,ujli.posting_period
            ,'' AS vcode
            ,ujli.company_code
            ,ujli.cost_center
            ,ujli.profit_center
            ,ujli.material
            ,ujli.customer
            ,ujli.segment
            ,ujli.subitem
            ,ujli.document_number
            ,ujli.reference_document_line_item
            -- Payload
            ,ujli.posting_item
            ,ujli.controlling_area
            ,ujli.division
            ,ujli.asset
            ,ujli.payer
            ,ujli.sales_organization
            ,ujli.ship_to_party
            ,ujli.bill_to_party
            ,ujli.distribution_channel
            ,ujli.plant
            ,ujli.supplier
            ,ujli.purchasing_document_item
            ,ujli.sales_order
            ,ujli.document_date
            ,ujli.document_type
            ,ujli.document_status
            ,ujli.sales_order_item
            ,ujli.invoice_reference
            ,ujli.item_category
            ,ujli.subitem_category
            ,ujli.purchasing_document
            ,ujli.debit_credit_ind
            ,ujli.financial_statement_item
            ,ujli.ledger
            ,ujli.business_area
            ,ujli.base_unit_of_measure
            ,ujli.additional_unit_of_measure_1
            ,ujli.additional_unit_of_measure_2
            ,ujli.reference_document
            ,ujli.transaction_type
            ,ujli.reference_org_unit
            ,ujli.transaction_type_gl
            ,ujli.product_sold_group
            ,ujli.transaction_currency
            ,ujli.company_code_currency
            ,ujli.global_currency
            ,'' AS elimination_flag
            -- Amounts
            ,ujli.quantity
            ,ujli.additional_quantity_1
            ,ujli.additional_quantity_2
            ,ujli.amount_in_global_currency       AS amount_in_group_currency
            ,ujli.amount_in_company_code_currency AS amount_in_local_currency
            ,ujli.amount_in_company_code_currency AS vcode_amount_local
            ,ujli.amount_in_global_currency       AS vcode_amount_group
            ,CASE
                WHEN ujli.document_type = '19' THEN True
                ELSE False
            END AS ifrs_flag
        FROM
            universal_journal_line_items_filtered AS ujli
    UNION ALL
        SELECT
            'ujci'  AS _indicator
            -- Keys 
            ,'Actual' AS actual_plan_code
            ,ujci.datasource AS datasource
            ,'' AS journal_type
            ,ujci.posting_date
            ,ujci.gl_account
            ,ujci.chart_of_accounts
            ,'' AS account_type
            ,'' AS offsetting_account_number
            ,ujci.fiscal_year_period
            ,ujci.fiscal_year
            ,ujci.posting_period
            ,ujci.vcode AS vcode
            ,ujci.company_code
            ,ujci.cost_center
            ,ujci.profit_center
            ,ujci.material_number AS material
            ,ujci.customer
            ,ujci.segment
            ,ujci.subitem
            ,ujci.document_number
            ,0 AS reference_document_line_item
            -- Payload
            ,ujci.posting_item
            ,ujci.controlling_area
            ,ujci.division
            ,'' AS asset
            ,'' AS payer
            ,ujci.sales_organization
            ,ujci.ship_to_party
            ,ujci.bill_to_party
            ,ujci.distribution_channel
            ,ujci.plant
            ,ujci.supplier
            ,0 AS purchasing_document_item
            ,'' AS sales_order
            ,'1900-01-01' AS document_date
            ,ujci.document_type
            ,'' AS document_status
            ,0 AS sales_order_item
            ,'' AS invoice_reference
            ,'' AS item_category
            ,ujci.subitem_category
            ,'' AS purchasing_document
            ,'' AS debit_credit_ind
            ,ujci.financial_statement_item
            ,ujci.ledger
            ,'' AS business_area
            ,ujci.base_unit_of_measure
            ,'' AS additional_unit_of_measure_1
            ,'' AS additional_unit_of_measure_2
            ,'' AS reference_document
            ,'' AS transaction_type
            ,'' AS reference_org_unit
            ,'' AS transaction_type_gl
            ,ujci.product_sold_group
            ,ujci.transaction_currency
            ,ujci.local_currency AS company_code_currency
            ,'USD' AS global_currency
            ,ujci.elimination_flag
            -- Amounts
            ,ujci.quantity
            ,ujci.quantity AS additional_quantity_1
            ,0             AS additional_quantity_2
            ,ujci.amount_in_group_currency
            ,ujci.amount_in_local_currency
            ,ujci.amount_in_local_currency   AS vcode_amount_local
            ,ujci.amount_in_group_currency   AS vcode_amount_group
            ,CASE
                WHEN ujci.document_type = '19' THEN True
                ELSE False
            END AS ifrs_flag
        FROM
            universal_journal_consolidation_items AS ujci
''')

combined_universal_journal_line_items.createOrReplaceTempView('combined_universal_journal_line_items')

# COMMAND ----------

combined_universal_journal_items_filtered = spark.sql(f'''
    SELECT 
        cuji.*
    FROM 
        combined_universal_journal_line_items AS cuji
    WHERE cuji.company_code != 'MA04'
        OR (
            cuji.company_code = 'MA04'
            AND
            cuji.datasource = 'S4HANA'
            AND 
            cuji.posting_date >= '2025-05-01' 
        )
        OR (
            cuji.company_code = 'MA04'
            AND 
            cuji.datasource LIKE 'SGR%'
        )    
    /*
    Note from Ricardo 30-06-2025

    Note that MA04 went live on 1 May 2025.  Amounts in SAP should be excluded for the period prior to that as they relate to taken-on balances and the full amount is already reported from SGR. 
    */
''')

combined_universal_journal_items_filtered.createOrReplaceTempView('combined_universal_journal_items_filtered')

# COMMAND ----------

universal_journal_line_items_extended = spark.sql(f'''
        SELECT
            ujli.* EXCEPT(
                ujli.vcode
                ,ujli.profit_center
            )
            ,CASE
                WHEN ujli.debit_credit_ind = 'H' THEN 'Credit'
                WHEN ujli.debit_credit_ind = 'S' THEN 'Debit'
                ELSE 'Unknown'
            END AS debit_credit_description
            ,CASE
                WHEN ujli.datasource IN ('SGR', 'SGRJNLS')
                    THEN ujli.vcode
                ELSE coalesce(vc.vcode, '') 
            END AS vcode
            ,glam.gl_account_type
            ,glam.balance_sheet_account_flag
            ,coalesce(pc.line_of_business, '')         AS line_of_business
            ,coalesce(pc.line_of_business_1, '')       AS line_of_business_1
            ,pc.profit_center
            ,pc.volume_flag_ind
            ,cc.cost_center_category
            -- Volumes
            ,CASE
                WHEN ujli.gl_account IN ('0100000010', '0100000021')
                        AND pc.volume_flag_ind = True
                    THEN ujli.additional_quantity_1
                ELSE 0
            END AS volume_litres_l20
            ,CASE
                WHEN ujli.gl_account IN ('0100000010', '0100000021')
                        AND pc.line_of_business = 'LOB_LP'
                        AND ujli.product_sold_group IN ('HCLPGAS', 'HCPKDLPG')
                    THEN ujli.additional_quantity_2
                ELSE 0
            END AS volume_kg
            ,CASE
                WHEN ujli.gl_account = '0120100010'
                        AND pc.volume_flag_ind = True
                    THEN ujli.additional_quantity_1
                ELSE 0
            END AS volume_issued_litres_l20
        FROM
            combined_universal_journal_items_filtered AS ujli
            LEFT JOIN profit_center AS pc
                ON ujli.profit_center = pc.profit_center
            LEFT JOIN cost_center AS cc
                ON ujli.cost_center = cc.cost_center
                AND cc.valid_to = '9999-12-31' 
            LEFT JOIN gl_account_master AS glam
                ON ujli.gl_account = glam.gl_account
            LEFT JOIN vcode_mapping AS vc
                ON (
                    CASE
                    WHEN (   glam.balance_sheet_account_flag = 'X'
                                OR glam.gl_account_type IN ('N', 'X')
                                OR ujli.cost_center = ''
                            )
                        THEN ujli.gl_account
                    ELSE
                        CASE
                            WHEN glam.gl_account_type IN ('P', 'S')
                                THEN ujli.gl_account || '_' || cc.cost_center_category
                            ELSE ''
                        END
                    END
                ) = vc.join_code
''')

universal_journal_line_items_extended.createOrReplaceTempView('universal_journal_line_items_extended')

# COMMAND ----------

with_consolidation_historic = spark.sql(f'''
        SELECT
            ujlie._indicator
            -- Keys 
            ,ujlie.actual_plan_code
            ,ujlie.datasource
            ,ujlie.journal_type
            ,ujlie.posting_date
            ,ujlie.gl_account
            ,ujlie.chart_of_accounts
            
            ,ujlie.vcode
            ,ujlie.company_code
            ,ujlie.cost_center
            ,ujlie.profit_center
            ,ujlie.material
            ,ujlie.customer
            ,ujlie.segment
            ,ujlie.subitem
            ,ujlie.document_number
            ,ujlie.reference_document_line_item
            ,ujlie.line_of_business
            ,ujlie.line_of_business_1
            -- Payload
            ,ujlie.gl_account_type

            ,ujlie.posting_item
            ,ujlie.controlling_area
            ,ujlie.division
            ,ujlie.asset
            ,ujlie.payer
            ,ujlie.sales_organization
            ,ujlie.ship_to_party
            ,ujlie.bill_to_party
            ,ujlie.distribution_channel
            ,ujlie.plant
            ,ujlie.supplier
            ,ujlie.purchasing_document_item
            ,ujlie.sales_order
            ,ujlie.document_date
            ,ujlie.document_type
            ,ujlie.document_status
            ,ujlie.sales_order_item
            ,ujlie.invoice_reference
            ,ujlie.item_category
            ,ujlie.subitem_category
            ,ujlie.purchasing_document
            ,ujlie.debit_credit_ind
            ,ujlie.financial_statement_item
            ,ujlie.ledger
            ,ujlie.business_area
            ,ujlie.base_unit_of_measure
            ,ujlie.additional_unit_of_measure_1
            ,ujlie.additional_unit_of_measure_2
            ,ujlie.reference_document
            ,ujlie.transaction_type
            ,ujlie.reference_org_unit
            ,ujlie.transaction_type_gl
            ,ujlie.product_sold_group
            ,ujlie.debit_credit_description
            ,ujlie.balance_sheet_account_flag
            ,ujlie.transaction_currency
            ,ujlie.company_code_currency
            ,ujlie.global_currency
            ,ujlie.elimination_flag
            ,ujlie.quantity
            ,ujlie.volume_kg
            ,ujlie.volume_litres_l20
            ,ujlie.volume_issued_litres_l20
            ,ujlie.amount_in_group_currency
            ,ujlie.amount_in_local_currency
            ,ujlie.vcode_amount_local
            ,ujlie.vcode_amount_group
            ,ujlie.account_type
            ,ujlie.offsetting_account_number
            ,ujlie.fiscal_year_period
            ,ujlie.fiscal_year
            ,ujlie.posting_period
            ,ujlie.ifrs_flag
        FROM
            universal_journal_line_items_extended AS ujlie
    UNION ALL
        SELECT
            'ftch' AS _indicator
            -- Keys
            ,ftch.actual_plan_code
            ,ftch.datasource
            ,ftch.journal_type
            ,ftch.posting_date
            ,ftch.gl_account
            ,'OP01' AS chart_of_accounts
            ,ftch.vcode AS vcode
            ,ftch.company_code
            ,ftch.cost_center
            ,ftch.profit_center
            ,'' AS material
            ,'' AS customer
            ,'' AS segment
            ,'' AS subitem
            ,'' AS document_number
            ,0 AS reference_document_line_item
            ,'' AS line_of_business
            ,'' AS line_of_business_1

            -- Payload
            ,'' AS gl_account_type
            
            ,'' AS posting_item
            ,ftch.controlling_area
            ,'' AS division
            ,'' AS asset
            ,'' AS payer
            ,'' AS sales_organization
            ,'' AS ship_to_party
            ,'' AS bill_to_party
            ,'' AS distribution_channel
            ,'' AS plant
            ,'' AS supplier
            ,0 AS purchasing_document_item
            ,'' AS sales_order
            ,'1900-01-01' AS document_date
            ,'' AS document_type
            ,'' AS document_status
            ,0 AS sales_order_item
            ,'' AS invoice_reference
            ,'' AS item_category
            ,'' AS subitem_category
            ,'' AS purchasing_document
            ,'' AS debit_credit_ind
            ,'' AS financial_statement_item
            ,'' AS ledger
            ,'' AS business_area
            ,'' AS base_unit_of_measure
            ,'' AS additional_unit_of_measure_1
            ,'' AS additional_unit_of_measure_2
            ,'' AS reference_document
            ,'' AS transaction_type
            ,'' AS reference_org_unit
            ,'' AS transaction_type_gl
            
            ,'' AS product_sold_group
            ,'' AS debit_credit_description
            ,False AS balance_sheet_account_flag
            ,'' AS transaction_currency
            ,ftch.currency_key AS company_code_currency
            ,'USD' AS global_currency
            ,'' AS elimination_flag
            -- Amounts
            ,ftch.quantity
            ,ftch.volume_kg
            ,ftch.volume_litres_l20
            ,0 AS volume_issued_litres_l20
            ,0 AS amount_in_group_currency
            ,ftch.amount_local_currency AS amount_in_local_currency
            ,ftch.vcode_amount_local
            ,0 AS vcode_amount_group
            ,'' AS account_type
            ,'' AS offsetting_account_number
            ,ftch.fiscal_year_period
            ,ftch.fiscal_year
            ,ftch.posting_period
            ,ftch.ifrs_flag
    FROM
        finance_transactions_consolidation_historic AS ftch
''')

with_consolidation_historic.createOrReplaceTempView('with_consolidation_historic')

# COMMAND ----------

v001_volume_lines = spark.sql(f'''
    WITH profit_and_loss AS (
        SELECT
            wch.*
        FROM
            with_consolidation_historic AS wch
        WHERE
            wch.volume_issued_litres_l20 <> 0
            OR wch.volume_litres_l20 <> 0
    )
    SELECT
        'vol_l20' AS _indicator
        ,pl.* EXCEPT(
                    _indicator,
                    vcode,
                    quantity,
                    volume_kg,
                    volume_litres_l20,
                    volume_issued_litres_l20,
                    amount_in_local_currency,
                    amount_in_group_currency,
                    vcode_amount_local,
                    vcode_amount_group
                )
        ,'V.001'           AS vcode
        ,0                 AS quantity
        ,0                 AS volume_kg
        ,0                 AS volume_litres_l20
        ,0                 AS volume_issued_litres_l20
        ,0                 AS amount_in_local_currency
        ,0                 AS amount_in_group_currency
        ,volume_litres_l20 AS vcode_amount_local
        ,volume_litres_l20 AS vcode_amount_group
    FROM
        profit_and_loss AS pl
''')

v001_volume_lines.createOrReplaceTempView('v001_volume_lines')

# COMMAND ----------

v002_volume_lines = spark.sql(f'''
    WITH profit_and_loss AS (
        SELECT
            wch.*
        FROM
            with_consolidation_historic AS wch
        WHERE
            wch.volume_kg <> 0
    )
    SELECT
        'vol_kg'   AS _indicator
        ,pl.* EXCEPT(
                    _indicator,
                    vcode,
                    quantity,
                    volume_kg,
                    volume_litres_l20,
                    volume_issued_litres_l20,
                    amount_in_local_currency,
                    amount_in_group_currency,
                    vcode_amount_local,
                    vcode_amount_group
                )
        ,'V.002'   AS vcode
        ,0         AS quantity
        ,0         AS volume_kg
        ,0         AS volume_litres_l20
        ,0         AS volume_issued_litres_l20
        ,0         AS amount_in_local_currency
        ,0         AS amount_in_group_currency
        ,volume_kg AS vcode_amount_local
        ,volume_kg AS vcode_amount_group
    FROM
        profit_and_loss AS pl
''')

v002_volume_lines.createOrReplaceTempView('v002_volume_lines')

# COMMAND ----------

volume_lines = spark.sql(f'''
        SELECT vl.*
        FROM v001_volume_lines AS vl
    UNION ALL
        SELECT vk.*
        FROM v002_volume_lines AS vk
''')
volume_lines.createOrReplaceTempView('volume_lines')

# COMMAND ----------

enhanced = spark.sql(f'''
        SELECT
            v._indicator
            ,coalesce(v.actual_plan_code, '') AS actual_plan_code
            ,coalesce(v.datasource, '') AS datasource
            ,coalesce(v.journal_type, '') AS journal_type
            ,coalesce(v.posting_date, '') AS posting_date
            ,coalesce(v.gl_account, '') AS gl_account
            ,coalesce(v.chart_of_accounts, '') AS chart_of_accounts
            ,coalesce(v.company_code, '') AS company_code
            ,coalesce(v.cost_center, '') AS cost_center
            ,coalesce(v.profit_center, '') AS profit_center
            ,coalesce(v.ifrs_flag, False) AS ifrs_flag
            ,coalesce(v.material, '') AS material
            ,coalesce(v.customer, '') AS customer
            ,coalesce(v.segment, '') AS segment
            ,v.gl_account_type
            ,v.line_of_business
            ,v.line_of_business_1
            ,coalesce(v.document_number, '') AS document_number
            ,coalesce(v.posting_item, '') AS posting_item
            ,v.controlling_area
            ,v.division
            ,v.asset
            ,v.payer
            ,v.sales_organization
            ,v.ship_to_party
            ,v.bill_to_party
            ,v.distribution_channel
            ,v.plant
            ,coalesce(v.subitem, '') AS subitem
            ,v.supplier
            ,v.purchasing_document_item
            ,v.sales_order
            ,v.document_date
            ,coalesce(v.document_type, '') AS document_type
            ,v.document_status
            ,v.sales_order_item
            ,v.invoice_reference
            ,v.item_category
            ,v.subitem_category
            ,v.purchasing_document
            ,v.debit_credit_ind
            ,v.financial_statement_item
            ,coalesce(v.ledger, '') AS ledger
            ,v.business_area
            ,v.base_unit_of_measure
            ,v.additional_unit_of_measure_1
            ,v.additional_unit_of_measure_2
            ,v.reference_document
            ,v.transaction_type
            ,v.reference_org_unit
            ,v.transaction_type_gl
            ,v.reference_document_line_item
            ,v.product_sold_group
            ,v.debit_credit_description
            ,v.balance_sheet_account_flag
            ,v.transaction_currency
            ,v.company_code_currency
            ,v.global_currency
            ,v.elimination_flag
            ,coalesce(v.vcode, '') AS vcode
            ,v.quantity
            ,v.volume_kg
            ,v.volume_litres_l20
            ,v.volume_issued_litres_l20
            ,v.amount_in_local_currency AS amount_in_company_code_currency
            ,v.amount_in_group_currency AS amount_in_global_currency
            ,v.vcode_amount_local
            ,v.vcode_amount_group
            ,coalesce(v.account_type, '') AS account_type
            ,coalesce(v.offsetting_account_number, '') AS offsetting_account_number
            ,coalesce(v.fiscal_year_period, 0) AS fiscal_year_period
            ,coalesce(v.fiscal_year, 0) AS fiscal_year
            ,coalesce(v.posting_period, 0) AS posting_period
        FROM
            volume_lines AS v
    UNION ALL
        SELECT
            cujli._indicator
            ,coalesce(cujli.actual_plan_code, '') AS actual_plan_code
            ,coalesce(cujli.datasource, '') AS datasource
            ,coalesce(cujli.journal_type, '') AS journal_type
            ,coalesce(cujli.posting_date, '') AS posting_date
            ,coalesce(cujli.gl_account, '') AS gl_account
            ,coalesce(cujli.chart_of_accounts, '') AS chart_of_accounts
            ,coalesce(cujli.company_code, '') AS company_code
            ,coalesce(cujli.cost_center, '') AS cost_center
            ,coalesce(cujli.profit_center, '') AS profit_center
            ,coalesce(cujli.ifrs_flag, False) AS ifrs_flag
            ,coalesce(cujli.material, '') AS material
            ,coalesce(cujli.customer, '') AS customer
            ,coalesce(cujli.segment, '') AS segment
            ,cujli.gl_account_type
            ,cujli.line_of_business
            ,cujli.line_of_business_1
            ,coalesce(cujli.document_number, '') AS document_number
            ,coalesce(cujli.posting_item, '') AS posting_item
            ,cujli.controlling_area
            ,cujli.division
            ,cujli.asset
            ,cujli.payer
            ,cujli.sales_organization
            ,cujli.ship_to_party
            ,cujli.bill_to_party
            ,cujli.distribution_channel
            ,cujli.plant
            ,coalesce(cujli.subitem, '') AS subitem
            ,cujli.supplier
            ,cujli.purchasing_document_item
            ,cujli.sales_order
            ,cujli.document_date
            ,coalesce(cujli.document_type, '') AS document_type
            ,cujli.document_status
            ,cujli.sales_order_item
            ,cujli.invoice_reference
            ,cujli.item_category
            ,cujli.subitem_category
            ,cujli.purchasing_document
            ,cujli.debit_credit_ind
            ,cujli.financial_statement_item
            ,coalesce(cujli.ledger, '') AS ledger
            ,cujli.business_area
            ,cujli.base_unit_of_measure
            ,cujli.additional_unit_of_measure_1
            ,cujli.additional_unit_of_measure_2
            ,cujli.reference_document
            ,cujli.transaction_type
            ,cujli.reference_org_unit
            ,cujli.transaction_type_gl
            ,cujli.reference_document_line_item
            ,cujli.product_sold_group
            ,cujli.debit_credit_description
            ,cujli.balance_sheet_account_flag
            ,cujli.transaction_currency
            ,cujli.company_code_currency
            ,cujli.global_currency
            ,cujli.elimination_flag
            ,coalesce(cujli.vcode, '') AS vcode
            ,cujli.quantity
            ,cujli.volume_kg
            ,cujli.volume_litres_l20
            ,cujli.volume_issued_litres_l20
            ,cujli.amount_in_local_currency AS amount_in_company_code_currency 
            ,cujli.amount_in_group_currency AS amount_in_global_currency
            ,cujli.vcode_amount_local
            ,cujli.vcode_amount_group
            ,coalesce(cujli.account_type, '') AS account_type
            ,coalesce(cujli.offsetting_account_number, '') AS offsetting_account_number
            ,coalesce(cujli.fiscal_year_period, 0) AS fiscal_year_period
            ,coalesce(cujli.fiscal_year, 0) AS fiscal_year
            ,coalesce(cujli.posting_period, 0) AS posting_period
        FROM
            with_consolidation_historic AS cujli
''')

enhanced.createOrReplaceTempView('enhanced')

# COMMAND ----------

write_result = metadata.process_transformation_table('enhanced', env_vars.silver_catalog)
logger.log.info(f'Write: {dest_tablename} {write_result}')