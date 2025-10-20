# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.silver_catalog}.fin_general_ledger.universal_journal_line_items (
        -- Keys,
        actual_plan_code STRING
            COMMENT 'Actual Plan Code',
        datasource STRING
            COMMENT 'Datasource',
        journal_type STRING
            COMMENT 'Journal Type',
        posting_date DATE
            COMMENT 'Posting Date',
        gl_account STRING
            COMMENT 'Gl Account',
        chart_of_accounts STRING
            COMMENT 'Chart Of Accounts',
        company_code STRING
            COMMENT 'Company Code',
        cost_center STRING
            COMMENT 'Cost Center',
        profit_center STRING
            COMMENT 'Profit Center',
        ifrs_flag BOOLEAN
            COMMENT 'IFRS Flag',
        material STRING
            COMMENT 'Material',
        customer STRING
            COMMENT 'Customer',
        segment STRING
            COMMENT 'Segment',
        subitem STRING
            COMMENT 'Subitem',
        item_category STRING
            COMMENT 'Item Category',
        subitem_category STRING
            COMMENT 'Subitem Category',
        -- Payload,
        gl_account_type STRING
            COMMENT 'Gl Account Type',
        line_of_business STRING
            COMMENT 'Line of business',
        line_of_business_1 STRING
            COMMENT 'Line of business 1',
        document_number STRING
            COMMENT 'Document Number',
        posting_item STRING
            COMMENT 'Posting Item',
        controlling_area STRING
            COMMENT 'Controlling Area',
        division STRING
            COMMENT 'Division',
        asset STRING
            COMMENT 'Asset',
        payer STRING
            COMMENT 'Payer',
        sales_organization STRING
            COMMENT 'Sales Organization',
        ship_to_party STRING
            COMMENT 'Ship To Party',
        bill_to_party STRING
            COMMENT 'Bill To Party',
        distribution_channel STRING
            COMMENT 'Distribution Channel',
        plant STRING
            COMMENT 'Plant',
        supplier STRING
            COMMENT 'Supplier',
        purchasing_document_item INT
            COMMENT 'Purchasing Document Item',
        sales_order STRING
            COMMENT 'Sales Order',
        document_date DATE
            COMMENT 'Document Date',
        document_type STRING
            COMMENT 'Document Type',
        document_status STRING
            COMMENT 'Document Status',
        sales_order_item INT
            COMMENT 'Sales Order Item',
        invoice_reference STRING
            COMMENT 'Invoice Reference',
        purchasing_document STRING
            COMMENT 'Purchasing Document',
        debit_credit_ind STRING
            COMMENT 'Debit Credit Ind',
        financial_statement_item STRING
            COMMENT 'Financial Statement Item',
        ledger STRING
            COMMENT 'Ledger',
        business_area STRING
            COMMENT 'Business Area',
        base_unit_of_measure STRING
            COMMENT 'Base Unit Of Measure',
        additional_unit_of_measure_1 STRING
            COMMENT 'Additional Unit Of Measure 1',
        additional_unit_of_measure_2 STRING
            COMMENT 'Additional Unit Of Measure 2',
        reference_document STRING
            COMMENT 'Reference Document',
        transaction_type STRING
            COMMENT 'Transaction Type',
        reference_org_unit STRING
            COMMENT 'Reference Org Unit',
        transaction_type_gl STRING
            COMMENT 'Transaction Type Gl',
        reference_document_line_item INT
            COMMENT 'Reference Document Line Item',
        product_sold_group STRING
            COMMENT 'Product Sold Group',
        debit_credit_description STRING
            COMMENT 'Debit Credit Description',
        balance_sheet_account_flag BOOLEAN
            COMMENT 'Balance Sheet Account Flag',
        transaction_currency STRING
            COMMENT 'Transaction Currency',
        company_code_currency STRING
            COMMENT 'Company Code Currency',
        global_currency STRING
            COMMENT 'Company Code Currency',
        elimination_flag STRING
            COMMENT 'Elimination Flag',
        vcode STRING
            COMMENT 'Vcode',
        quantity DECIMAL(23,4)
            COMMENT 'Quantity',
        volume_kg DECIMAL(23,4)
            COMMENT 'Volume Kg',
        volume_litres_l20 DECIMAL(23,4)
            COMMENT 'Volume Litres L20',
        volume_issued_litres_l20 DECIMAL(23,4)
            COMMENT 'Volume Issued Litres L20',
        amount_in_company_code_currency DECIMAL(28,8)
            COMMENT 'Amount In Company Code Currency',
        amount_in_global_currency DECIMAL(23,4)
            COMMENT 'Amount In Global Currency',
        vcode_amount_local DECIMAL(23,4)
            COMMENT 'Vcode Amount Local',
        vcode_amount_group DECIMAL(23,4)
            COMMENT 'Vcode Amount Group',
        account_type STRING 
            COMMENT "Account Type",
        offsetting_account_number STRING 
            COMMENT "Offsetting Account Number",
        fiscal_year_period INT 
            COMMENT "Fiscal Year Period",
        fiscal_year INT 
            COMMENT "Fiscal Year",
        posting_period INT 
            COMMENT "Posting Period",


        -- metadata
        __etl_keys_fprint BIGINT
            COMMENT 'xxhash64 of the Business Keys that this record is made up of (or, where the table is the result of joins, it is the fields that make the record unique)',
        __etl_row_fprint BIGINT
            COMMENT ' the xxhash64 of all the columns that make up the row payload (i.e. all the non-key, and non-metadata columns). Note: all columns must be NOT NULL for the hash to calculate properly.',
        __etl_effective_from DATE
            COMMENT 'date (as DATE datatype) that row is effective from. For an updated record this is the previous _effective_to date + 1 day.',
        __etl_effective_to DATE
            COMMENT 'date (as DATE datatype) that row is effective to, or NULL for active record',
        __etl_is_active BOOLEAN
            COMMENT 'boolean flag indicating the active record. Note: there should only be 1 _is_active for any _etl_keys_fprint',
        __etl_is_deleted BOOLEAN
            COMMENT 'boolean showing if the record has been deleted from the source system'
        )
        COMMENT "Universal Journal - Actuals is the heart of accounting in SAP ERP. All financial related transactions from all the SAP subadministrations (modules) are captured here. In VivID only the leading ledger (\'0L\') is taken into silver and gold."
        CLUSTER BY
            AUTO;
''')