# Databricks notebook source
# MAGIC %md
# MAGIC ## silver_schema_ddl
# MAGIC
# MAGIC Idempotent script to set up the Catalog and Schemas for the Silver tier.
# MAGIC
# MAGIC Schemas agreed as per:  
# MAGIC https://dev.azure.com/VivoEnergy/Azure%20SAP%20Data%20Reporting/_wiki/wikis/Azure-SAP-Data-Reporting.wiki/459/RFC-7-Vivo-aligned-Schema-naming-standard
# MAGIC
# MAGIC <br />
# MAGIC
# MAGIC | Catalog Name | Catalog Description | Schema Name  | Schema Description | Tags |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | vivid_dev_slv | VIVID Dev Silver | ca_cross_application_components | Cross Application Components | Cross Application Components |
# MAGIC | vivid_dev_slv | VIVID Dev Silver | fin_finance | Finance | Workstream: Finance, Domain: Finance |
# MAGIC | vivid_dev_slv | VIVID Dev Silver | fin_accounts_payable | Accounts Payable | Workstream: Finance, Domain: Accounts Payable |
# MAGIC | vivid_dev_slv | VIVID Dev Silver | fin_accounts_receiveable | Accounts Receivable | Workstream: Finance, Domain: Accounts Receivable |
# MAGIC | vivid_dev_slv | VIVID Dev Silver | fin_asset_accounting | Asset Accounting | Workstream: Finance, Domain: Asset Accounting |
# MAGIC | vivid_dev_slv | VIVID Dev Silver | fin_cash_management | Cash Management | Workstream: Finance, Domain: Cash Management |
# MAGIC | vivid_dev_slv | VIVID Dev Silver | fin_controlling | Controlling | Workstream: Finance, Domain: Controlling |
# MAGIC | vivid_dev_slv | VIVID Dev Silver | fin_credit_management | Credit Management | Workstream: Finance, Domain: Credit Management |
# MAGIC | vivid_dev_slv | VIVID Dev Silver | fin_general_ledger | General Ledger | Workstream: Finance, Domain: General Ledger |
# MAGIC | vivid_dev_slv | VIVID Dev Silver | fin_procurement | Procurement | Workstream: Finance, Domain: Procurement |
# MAGIC | vivid_dev_slv | VIVID Dev Silver | fin_projects | Projects | Workstream: Finance, Domain: Projects |
# MAGIC | vivid_dev_slv | VIVID Dev Silver | fin_real_estate | Real Estate | Workstream: Finance, Domain: Real Estate |
# MAGIC | vivid_dev_slv | VIVID Dev Silver | fin_treasury | Treasury Management | Workstream: Finance, Domain: Treasury Management |
# MAGIC | vivid_dev_slv | VIVID Dev Silver | hr_human_resources | Human Resources | Workstream: Human Resources, Domain: Human Resources |
# MAGIC | vivid_dev_slv | VIVID Dev Silver | hr_payroll | Payroll | Workstream: Human Resources, Domain: Payroll |
# MAGIC | vivid_dev_slv | VIVID Dev Silver | hr_talent | Talent Attraction | Workstream: Human Resources, Domain: Talent Attraction |
# MAGIC | vivid_dev_slv | VIVID Dev Silver | sl_sales_logistics | Sales and Logistics | Workstream: Sales and Logistics, Domain: Sales and Logistics |
# MAGIC | vivid_dev_slv | VIVID Dev Silver | sl_cards | Cards | Workstream: Sales and Logistics, Domain: Cards |
# MAGIC | vivid_dev_slv | VIVID Dev Silver | sl_convenience_retailing | Convenience Retailing | Workstream: Sales and Logistics, Domain: Convenience Retailing |
# MAGIC | vivid_dev_slv | VIVID Dev Silver | sl_extended_warehouse_management | Extended Warehouse Management | Workstream: Sales and Logistics, Domain: Extended Warehouse Management |
# MAGIC | vivid_dev_slv | VIVID Dev Silver | sl_hydrocarbons_management | Hydrocarbons Management | Workstream: Sales and Logistics, Domain: Hydrocarbons Management |
# MAGIC | vivid_dev_slv | VIVID Dev Silver | sl_integrated_business_planning | Integraded Business Planning | Workstream: Sales and Logistics, Domain: Integraded Business Planning |
# MAGIC | vivid_dev_slv | VIVID Dev Silver | sl_order_execution_management | Order Execution Management | Workstream: Sales and Logistics, Domain: Order Execution Management |
# MAGIC | vivid_dev_slv | VIVID Dev Silver | sl_plant_maintenance | Plant Maintenance | Workstream: Sales and Logistics, Domain: Plant Maintenance |
# MAGIC | vivid_dev_slv | VIVID Dev Silver | sl_plan_to_manufacture | Plan to Manufacture | Workstream: Sales and Logistics, Domain: Plan to Manufacture |
# MAGIC | vivid_dev_slv | VIVID Dev Silver | sl_sales_and_marketing | Sales and Marketing | Workstream: Sales and Logistics, Domain: Sales and Marketing |
# MAGIC | vivid_dev_slv | VIVID Dev Silver | sl_secondary_distribution | Secondary Distribution Management | Workstream: Sales and Logistics, Domain: Secondary Distribution Management |

# COMMAND ----------

# MAGIC %run ../../common/properties

# COMMAND ----------

import yaml

# COMMAND ----------

# MAGIC %md
# MAGIC Catalogs

# COMMAND ----------

spark.sql(f'''
    CREATE CATALOG IF NOT EXISTS {env_vars.silver_catalog};
''')
# TODO: drop this, as this is now in Terraform

# COMMAND ----------

# MAGIC %md
# MAGIC Schemas for tables

# COMMAND ----------

schemas_text = '''
schemas:
  - name: ca_cross_application_components
    description: Cross Application Components
    tags:
      - key: Workstream
        value: Cross Application Components
  - name: fin_finance
    description: Finance
    tags:
      - key: Workstream
        value: Finance
      - key: Domain
        value: Finance
  - name: fin_accounts_payable
    description: Accounts Payable
    tags:
      - key: Workstream
        value: Finance
      - key: Domain
        value: Accounts Payable
  - name: fin_accounts_receiveable
    description: Accounts Receivable
    tags:
      - key: Workstream
        value: Finance
      - key: Domain
        value: Accounts Receivable
  - name: fin_asset_accounting
    description: Asset Accounting
    tags:
      - key: Workstream
        value: Finance
      - key: Domain
        value: Asset Accounting
  - name: fin_cash_management
    description: Cash Management
    tags:
      - key: Workstream
        value: Finance
      - key: Domain
        value: Cash Management
  - name: fin_controlling
    description: Controlling
    tags:
      - key: Workstream
        value: Finance
      - key: Domain
        value: Controlling
  - name: fin_credit_management
    description: Credit Management
    tags:
      - key: Workstream
        value: Finance
      - key: Domain
        value: Credit Management
  - name: fin_general_ledger
    description: General Ledger
    tags:
      - key: Workstream
        value: Finance
      - key: Domain
        value: General Ledger
  - name: fin_procurement
    description: Procurement
    tags:
      - key: Workstream
        value: Finance
      - key: Domain
        value: Procurement
  - name: fin_projects
    description: Projects
    tags:
      - key: Workstream
        value: Finance
      - key: Domain
        value: Projects
  - name: fin_real_estate
    description: Real Estate
    tags:
      - key: Workstream
        value: Finance
      - key: Domain
        value: Real Estate
  - name: fin_treasury
    description: Treasury Management
    tags:
      - key: Workstream
        value: Finance
      - key: Domain
        value: Treasury Management
  #
  # Human Resources
  #
  - name: hr_human_resources
    description: Human Resources
    tags:
      - key: Workstream
        value: Human Resources
      - key: Domain
        value: Human Resources
  - name: hr_payroll
    description: Payroll
    tags:
      - key: Workstream
        value: Human Resources
      - key: Domain
        value: Payroll
  - name: hr_talent
    description: Talent Attraction
    tags:
      - key: Workstream
        value: Human Resources
      - key: Domain
        value: Talent Attraction
  #
  # Sales and Logistics
  #
  - name: sl_sales_logistics
    description: Sales and Logistics
    tags:
      - key: Workstream
        value: Sales and Logistics
      - key: Domain
        value: Sales and Logistics
  - name: sl_cards
    description: Cards
    tags:
      - key: Workstream
        value: Sales and Logistics
      - key: Domain
        value: Cards
  - name: sl_convenience_retailing
    description: Convenience Retailing
    tags:
      - key: Workstream
        value: Sales and Logistics
      - key: Domain
        value: Convenience Retailing
  - name: sl_extended_warehouse_management
    description: Extended Warehouse Management
    tags:
      - key: Workstream
        value: Sales and Logistics
      - key: Domain
        value: Extended Warehouse Management
  - name: sl_hydrocarbons_management
    description: Hydrocarbons Management
    tags:
      - key: Workstream
        value: Sales and Logistics
      - key: Domain
        value: Hydrocarbons Management
  - name: sl_integrated_business_planning
    description: Integraded Business Planning
    tags:
      - key: Workstream
        value: Sales and Logistics
      - key: Domain
        value: Integraded Business Planning
  - name: sl_order_execution_management
    description: Order Execution Management
    tags:
      - key: Workstream
        value: Sales and Logistics
      - key: Domain
        value: Order Execution Management
  - name: sl_plant_maintenance
    description: Plant Maintenance
    tags:
      - key: Workstream
        value: Sales and Logistics
      - key: Domain
        value: Plant Maintenance
  - name: sl_plan_to_manufacture
    description: Plan to Manufacture
    tags:
      - key: Workstream
        value: Sales and Logistics
      - key: Domain
        value: Plan to Manufacture
  - name: sl_sales_and_marketing
    description: Sales and Marketing
    tags:
      - key: Workstream
        value: Sales and Logistics
      - key: Domain
        value: Sales and Marketing
  - name: sl_secondary_distribution
    description: Secondary Distribution Management
    tags:
      - key: Workstream
        value: Sales and Logistics
      - key: Domain
        value: Secondary Distribution Management
'''

# COMMAND ----------

schemas_yaml = yaml.safe_load(schemas_text)

# COMMAND ----------

# create the schemas for the tables

for s in schemas_yaml['schemas']:

    create_schema = f'''
        CREATE SCHEMA IF NOT EXISTS {env_vars.silver_catalog}.{s['name']}
        COMMENT "{s['description']}";
    '''

    spark.sql(create_schema)
    print(f'Created schema: {s["name"]}')

    all_tags = [f'''"{t['key']}" = "{t['value']}"'''  for t in s['tags']]
    tags_str = ','.join(all_tags)

    add_tags = f'''
        ALTER SCHEMA {env_vars.silver_catalog}.{s['name']}
        SET TAGS ({tags_str});
    '''

    spark.sql(add_tags)
    print(f'\tAdded tags: {tags_str}')

# COMMAND ----------

# create the schemas for the staing tables

for s in schemas_yaml['schemas']:

    create_schema = f'''
        CREATE SCHEMA IF NOT EXISTS {env_vars.silver_catalog}.{s['name']}_staging
        COMMENT "{s['description']}";
    '''

    spark.sql(create_schema)
    print(f'Created schema: {s["name"]}_staging')