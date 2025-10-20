# Databricks notebook source
# MAGIC %run ../../../common/properties
# MAGIC

# COMMAND ----------

spark.sql(f'''
    CREATE OR REPLACE TABLE {env_vars.silver_catalog}.fin_general_ledger.universal_journal_consolidation_items (
        client STRING
            COMMENT "Client",
        document_number STRING 
            COMMENT "Document Number",
        posting_item STRING 
            COMMENT "Posting Item",
        fiscal_year INT 
            COMMENT "Fiscal Year",
        dimension STRING 
            COMMENT "Dimension",
        ledger STRING 
            COMMENT "Ledger",
        original_compcode STRING 
            COMMENT "Original Company Code",
        company STRING
            COMMENT "Company",
        controlling_area STRING 
            COMMENT "Controlling Area",
        record_type STRING 
            COMMENT "Record Type",
        consolidation_version STRING 
            COMMENT "Consolidation Version",
        transaction_currency STRING
            COMMENT "Transaction Currency",
        local_currency STRING 
            COMMENT "Local Currency",
        posting_period INT 
            COMMENT "Posting Period",
        document_category STRING 
            COMMENT "Document Category",
        consolidation_unit STRING 
            COMMENT "Consolidation Unit",
        financial_statement_item STRING 
            COMMENT "Financial Statement Item",
        subitem_category STRING 
            COMMENT "Subitem Category",
        subitem STRING 
            COMMENT "Subitem",
        posting_level STRING 
            COMMENT "Posting Level",
        document_type STRING 
            COMMENT "Document Type",
        ledger_currency STRING 
            COMMENT "Ledger Currency",
        base_unit_of_measure STRING 
            COMMENT "Base Unit of Measure",
        gl_account STRING 
            COMMENT "GL Account",
        material_number STRING 
            COMMENT "Material Number",
        fiscal_year_period INT 
            COMMENT "Fiscal Year Period",
        cost_center STRING 
            COMMENT "Cost Center",
        profit_center STRING 
            COMMENT "Profit Center",
        date_created DATE 
            COMMENT "Date Created",
        chart_of_accounts STRING 
            COMMENT "Chart of Accounts",
        consolidation_chart_of_accounts STRING 
            COMMENT "Consolidation Chart of Accounts",
        ct_flag INT 
            COMMENT "CT Flag",
        ship_to_party STRING 
            COMMENT "Ship-to Party",
        bill_to_party STRING 
            COMMENT "Bill-to Party",
        customer_group STRING 
            COMMENT "Customer Group",
        product_sold STRING 
            COMMENT "Product Sold",
        product_sold_group STRING
            COMMENT "Product Sold Group",
        division STRING 
            COMMENT "Division",
        partner_unit STRING 
            COMMENT "Partner Unit",
        distribution_channel STRING 
            COMMENT "Distribution Channel",
        sales_organization STRING 
            COMMENT "Sales Organization",
        customer STRING 
            COMMENT "Customer",
        supplier STRING 
            COMMENT "Supplier",
        plant STRING 
            COMMENT "Plant",
        posting_date DATE 
            COMMENT "Posting Date",
        segment STRING 
            COMMENT "Segment",
        trading_partner_no STRING 
            COMMENT "Trading Partner No",
        datasource STRING
            COMMENT "Datasource",
        elimination_flag STRING 
            COMMENT "Elimination Flag",
        fs_item_mapping_id STRING 
            COMMENT "FS Item Mapping ID",
        fs_item_mapping_version STRING 
            COMMENT "FS Item Mapping Version",
        company_code STRING 
            COMMENT "Company Code",
        cost_center_category STRING 
            COMMENT "Cost Center Category",
        vcode STRING 
            COMMENT "VCode",
        amount_in_trans_currency DECIMAL(28,8)
            COMMENT "Amount in Trans Currency",
        quantity DECIMAL(23,4) 
            COMMENT "Quantity",
        amount_in_group_currency DECIMAL(28,8) 
            COMMENT "Amount in Group Currency",
        amount_in_local_currency DECIMAL(28,8) 
            COMMENT "Amount in Local Currency",
        time_created TIMESTAMP
            COMMENT "Time Created",
        entity_grouping_level_top STRING
            COMMENT 'Entity Grouping Level Top',
        month_end_date DATE
            COMMENT 'Month End Date',
        
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