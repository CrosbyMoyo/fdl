# Databricks notebook source
# MAGIC %run ../../../common/properties

# COMMAND ----------

dbutils.widgets.dropdown(
    name = 'table_operation',
    defaultValue = 'CREATE TABLE IF NOT EXISTS', 
    choices = [
        'CREATE TABLE IF NOT EXISTS',
        'CREATE OR REPLACE'
    ],
    label = '1- Table Operation'
)

table_operation = dbutils.widgets.get('table_operation')

# COMMAND ----------

table_name = 'grtransactiondata_q10'

# COMMAND ----------

spark,sql(f'''
    ALTER TABLE {env_vars.bronze_catalog}.sap_s4hana.{table_name}
    SET TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled');
''')

# COMMAND ----------

spark.sql(f'''
    -- CREATE TABLE IF NOT EXISTS
    {table_operation} {env_vars.bronze_catalog}.sap_s4hana.{table_name}
    (
        `ID` string,
        ConsolidationLedger string,
        ConsolidationLedger_Text string,
        FiscalYear string,
        ConsolidationDocumentNumber string,
        ConsolidationPostingItem string,
        GLRecordType string,
        GLRecordType_Text string,
        ConsolidationVersion string,
        ConsolidationVersion_Text string,
        TransactionCurrency string,
        TransactionCurrency_Text string,
        LocalCurrency string,
        LocalCurrency_Text string,
        GroupCurrency string,
        GroupCurrency_Text string,
        BaseUnit string,
        BaseUnit_Text string,
        FiscalPeriod string,
        FiscalYearPeriod string,
        FiscalYearVariant string,
        PeriodMode string,
        ConsolidationDocumentType string,
        ConsolidationDocumentType_Text string,
        DebitCreditCode string,
        DebitCreditCode_Text string,
        Company string,
        ConsolidationUnit string,
        ConsolidationUnit_Text string,
        ConsolidationUnitForElim string,
        ConsolidationUnitForElim_Text string,
        ConsolidationChartOfAccounts string,
        ConsolidationChartOfAccounts_Text string,
        FinancialStatementItem string,
        FinancialStatementItem_Text string,
        PartnerConsolidationUnit string,
        PartnerConsolidationUnit_Text string,
        ConsolidationGroup string,
        ConsolidationGroup_Text string,
        CompanyCode string,
        CompanyCode_Text string,
        SubItemCategory string,
        SubItemCategory_Text string,
        SubItem string,
        SubItem_Text string,
        PostingLevel string,
        PostingLevel_Text string,
        ConsolidationApportionment string,
        CurrencyConversionsDiffType string,
        CurrencyConversionsDiffType_Text string,
        ConsolidationAcquisitionYear string,
        ConsolidationAcquisitionPeriod string,
        InvesteeConsolidationUnit string,
        InvesteeConsolidationUnit_Text string,
        AmountInTransactionCurrency string,
        AmountInLocalCurrency string,
        AmountInGroupCurrency string,
        QuantityInBaseUnit string,
        CnsldtnQuantityInBaseUnit string,
        DocumentItemText string,
        ConsolidationPostgItemAutoFlag string,
        BusinessTransactionType string,
        PostingDate string,
        CurrencyTranslationDate string,
        RefConsolidationDocumentNumber string,
        ReferenceFiscalYear string,
        RefConsolidationPostingItem string,
        RefConsolidationDocumentType string,
        RefBusinessTransactionType string,
        CreationDate string,
        CreationTime string,
        UserID string,
        ReverseDocument string,
        ReversedDocument string,
        InvestmentActivityType string,
        InvestmentActivityType_Text string,
        InvestmentActivity string,
        ConsolidationDocReversalYear string,
        ReferenceDocumentType string,
        ReferenceDocumentContext string,
        LogicalSystem string,
        ChartOfAccounts string,
        ChartOfAccounts_Text string,
        GLAccount string,
        GLAccount_Text string,
        AssignmentReference string,
        CostCenter string,
        CostCenter_Text string,
        ProfitCenter string,
        ProfitCenter_Text string,
        ConsolidationPrftCtrForElim string,
        ConsolidationPrftCtrForElim_Text string,
        FunctionalArea string,
        FunctionalArea_Text string,
        BusinessArea string,
        BusinessArea_Text string,
        ControllingArea string,
        ControllingArea_Text string,
        Segment string,
        Segment_Text string,
        ConsolidationSegmentForElim string,
        ConsolidationSegmentForElim_Text string,
        PartnerCostCenter string,
        PartnerCostCenter_Text string,
        PartnerProfitCenter string,
        PartnerProfitCenter_Text string,
        PartnerFunctionalArea string,
        PartnerFunctionalArea_Text string,
        PartnerBusinessArea string,
        PartnerBusinessArea_Text string,
        PartnerCompany string,
        PartnerCompany_Text string,
        PartnerSegment string,
        PartnerSegment_Text string,
        OrderID string,
        OrderID_Text string,
        Customer string,
        Customer_Text string,
        Supplier string,
        Supplier_Text string,
        Material string,
        Material_Text string,
        Plant string,
        Plant_Text string,
        FinancialTransactionType string,
        FinancialTransactionType_Text string,
        WBSElementInternalID string,
        WBSDescription string,
        WBSElementExternalID string,
        WBSElementExternalID_Text string,
        Project string,
        Project_Text string,
        BillingDocumentType string,
        BillingDocumentType_Text string,
        SalesOrganization string,
        SalesOrganization_Text string,
        DistributionChannel string,
        DistributionChannel_Text string,
        OrganizationDivision string,
        OrganizationDivision_Text string,
        MaterialGroup string,
        MaterialGroup_Text string,
        SoldProduct string,
        SoldProduct_Text string,
        SoldProductGroup string,
        SoldProductGroup_Text string,
        CustomerGroup string,
        CustomerGroup_Text string,
        CustomerSupplierCountry string,
        CustomerSupplierCountry_Text string,
        CustomerSupplierIndustry string,
        CustomerSupplierIndustry_Text string,
        SalesDistrict string,
        SalesDistrict_Text string,
        BillToParty string,
        BillToParty_Text string,
        ShipToParty string,
        ShipToParty_Text string,
        CustomerSupplierCorporateGroup string,
        CnsldtnAdhocItem string,
        CnsldtnAdhocItemText string,
        CnsldtnAdhocSet string,
        CnsldtnAdhocSetText string,
        CnsldtnAdhocSetItem string,
        CnsldtnAdhocSetItemText string,

        -- metadata columns
        __etl_id BIGINT
            GENERATED ALWAYS AS IDENTITY,
        __etl_bronze_timestamp TIMESTAMP
            DEFAULT current_timestamp(),
        __etl_silver_timestamp TIMESTAMP,
        __etl_source_operation STRING
    )
    CLUSTER BY
        AUTO;
''')

# COMMAND ----------

# add the metadata columns back in
# spark.sql(f'''
#     ALTER TABLE {env_vars.bronze_catalog}.sap_s4hana.{table_name}
#     ADD COLUMNS (
#         __etl_id BIGINT,
#         __etl_bronze_timestamp TIMESTAMP,
#         __etl_silver_timestamp TIMESTAMP,
#         __etl_source_operation STRING
#     );
# ''')
