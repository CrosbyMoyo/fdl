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

table_name = 'vtbfha'

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
        MANDT string,
        BUKRS string,
        RFHA string,
        CRUSER string,
        DCRDAT string,
        TCRTIM string,
        UPUSER string,
        DUPDAT string,
        TUPTIM string,
        RANTYP string,
        SANLF string,
        SFGTYP string,
        SGSART string,
        SFHAART string,
        RGATT string,
        RMAID string,
        RFHAZUNR string,
        RFHAZUL string,
        SAKTIV string,
        KONTRH string,
        SROLEXT string,
        RGARANT string,
        RREFKONT string,
        RREFKONT2 string,
        RPORTB string,
        WGSCHFT string,
        WGSCHFT1 string,
        WGSCHFT2 string,
        AMTINPUT string,
        DBLFZ string,
        SKALID string,
        SKALID2 string,
        JLIMIT string,
        AKUEND string,
        SKUEND string,
        OBJNR string,
        RLDEPO string,
        ZUONR string,
        DELFZ string,
        ABWTYP string,
        TBEGRU string,
        VRFHA string,
        SINCLBE string,
        SNPVCAL string,
        SRNDNG string,
        ZUOND string,
        REFER string,
        MERKM string,
        SFRGZUST string,
        RANL string,
        RCOMVALCL string,
        FACILITYNR string,
        FACILITYBUKRS string,
        POSACC string,
        RCOMVALCL2 string,
        FUND string,
        GRANT_NBR string,
        TIMESTAMP_DEAL decimal(38,18),
        COMMODITY_ID string,
        RPORTB2 string,
        COMMODITY_ID1 string,
        COMMODITY_ID2 string,
        CLEARING_OPTION string,
        CLEARING_STATUS string,
        CLEARING_DATE string,
        EXT_ACCOUNT string,
        CLEAR_DATE_ACT string,
        SCONDITION string,
        RISK_MITIGATING string,
        FIMA_CALCULATION string,
        TRUSTEE string,
        PRCTR string,
        RCNTR string,
        PS_POSID string,
        RBUSA string,
        HEDGE_CLASS string,
        INIT_CLASSIFIER string,
        COUNTRY string,
        FB_SEGMENT string,
        BEHALF_OF_COMPANY string,
        TRADED_CURRENCY string,
        HEDGE_REQUEST_ID string,
        CFI_CODE string,
        ISIN string,
        MIC string,
        CONTRACT_TIMESTAMP_UTC decimal(38,18),
        BUPLA string,
        FKBER string,
        PRICEINDEX_USED string,
        HEDGE_BOOK string,
        HEDGE_SPECIFICATION_ID string,
        HEDGE_SPEC_ORDER_ID string,
        CUSTOM_DIFF_TERM_1 string,
        CUSTOM_DIFF_TERM_2 string,
        CUSTOM_DIFF_TERM_3 string,
        CUSTOM_DIFF_TERM_4 string,
        CUSTOM_DIFF_TERM_5 string,
        FLOWUUID_ACTIVE string,
        ZZ_NOT_DATE string,
        ZZ_BOL_DATE string,
        ZZ_BOL_RATE decimal(38,18),
        ZZ_PO_SO_NUMBER string,
        ZZ_VENDOR string,
        ZZ_VENDOR_NAME string,
        ZZ_EXT_INVOICE string,
        ZZ_TEXT string,
        ZZ_IBC string,
        ZZ_INT_LOAN_REF string,
        ZZ_BOP_CODE1 string,
        ZZ_BOP_CODE11 string,
        ZZ_BOP_AMOUNT1 decimal(38,18),
        ZZ_BOP_REASON1 string,
        ZZ_BOP_REF1 string,
        ZZ_BOP_AUT_DLR1 string,
        ZZ_BOP_AUT_DATE1 string,
        ZZ_IMPORT_CONTROL1 string,
        ZZ_CUSTOM_OFFICE1 string,
        ZZ_LOAN_REF1 string,
        ZZ_APPL_COUNTRY1 string,
        ZZ_ADHOC_SUBJECT1 string,
        ZZ_ADHOC_DESC1 string,
        ZZ_TRANSPORT_DOC1 string,
        ZZ_RFHA_MIR1 string,
        ZZ_COMPLIANCE_TYPE1 string,
        ZZ_SARB1 string,
        ZZ_CUSTCLIENTNO1 string,
        ZZ_BANK_TO_BANK1 string,
        ZZ_CLEARING_DATE1 string,
        ZZ_BOP_CODE2 string,
        ZZ_BOP_CODE22 string,
        ZZ_BOP_AMOUNT2 decimal(38,18),
        ZZ_BOP_REASON2 string,
        ZZ_BOP_REF2 string,
        ZZ_BOP_AUT_DLR2 string,
        ZZ_BOP_AUT_DATE2 string,
        ZZ_IMPORT_CONTROL2 string,
        ZZ_CUSTOM_OFFICE2 string,
        ZZ_LOAN_REF2 string,
        ZZ_APPL_COUNTRY2 string,
        ZZ_ADHOC_SUBJECT2 string,
        ZZ_ADHOC_DESC2 string,
        ZZ_TRANSPORT_DOC2 string,
        ZZ_RFHA_MIR2 string,
        ZZ_COMPLIANCE_TYPE2 string,
        ZZ_SARB2 string,
        ZZ_CUSTCLIENTNO2 string,
        ZZ_BANK_TO_BANK2 string,
        ZZ_CLEARING_DATE2 string,
        ZZ_BOP_CODE3 string,
        ZZ_BOP_CODE33 string,
        ZZ_BOP_AMOUNT3 decimal(38,18),
        ZZ_BOP_REASON3 string,
        ZZ_BOP_REF3 string,
        ZZ_BOP_AUT_DLR3 string,
        ZZ_BOP_AUT_DATE3 string,
        ZZ_IMPORT_CONTROL3 string,
        ZZ_CUSTOM_OFFICE3 string,
        ZZ_LOAN_REF3 string,
        ZZ_APPL_COUNTRY3 string,
        ZZ_ADHOC_SUBJECT3 string,
        ZZ_ADHOC_DESC3 string,
        ZZ_TRANSPORT_DOC3 string,
        ZZ_RFHA_MIR3 string,
        ZZ_COMPLIANCE_TYPE3 string,
        ZZ_SARB3 string,
        ZZ_CUSTCLIENTNO3 string,
        ZZ_BANK_TO_BANK3 string,
        ZZ_CLEARING_DATE3 string,

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
