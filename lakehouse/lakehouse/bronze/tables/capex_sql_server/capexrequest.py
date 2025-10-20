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

table_name = 'capexrequest'

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
        `Id` int,
        UserId int,
        CountryId int,
        LineOfBusinessId int,
        ModelTypeId int,
        ModelSubTypeId int,
        CompanyId int,
        CapexRequestTypeId int,
        ProjectName string,
        RequestAmountCurrentYear decimal(36,12),
        RequestAmountNextYear decimal(36,12),
        LeaseRepayments decimal(36,12),
        FundingPartners decimal(36,12),
        LongTermCommitments decimal(36,12),
        AnnualContractRevenue decimal(36,12),
        ContractDuration int,
        RequestYear int,
        CoSiteCount int,
        DoSiteCount int,
        TextArea1 string,
        TextArea2 string,
        TextArea3 string,
        InternalOrder string,
        IsHsseInvestment boolean,
        IsSpecialProject boolean,
        Active boolean,
        CapexRequestProjectTypesId int,
        CreatedBy string,
        Created timestamp,
        LastModifiedBy string,
        LastModified timestamp,
        NBV decimal(36,12),
        NPV decimal(36,12),
        IRR decimal(36,12),
        POT decimal(36,12),
        ROACE decimal(36,12),
        SAPCodeId int,
        PlatformTypeId int,
        SiteTypeId int,
        TotalCylinders int,
        Deposits decimal(36,12),
        WorkingCapital decimal(36,12),

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
