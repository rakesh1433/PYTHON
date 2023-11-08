CREATE OR REPLACE TABLE
  `@derived_project.product_gl.pc_future_dated_transaction_short_rate_staging` ( policy_period_public_id String,
    accounting_date DATETIME,
    policy_number String,
    term_number Int64,
    model_number Int64,
    cost_fixed_id String,
    cost_public_id String,
    coverage_pattern_code String,
    orig_coverage_pattern_code String,
    coverage_fixed_id String ) ;



CREATE OR REPLACE TABLE
  `@derived_project.product_gl.sr_trans_staging` ( 
    policy_account_number STRING,
    policy_number STRING,
    policy_period_start_date DATETIME,
    policy_period_end_date DATETIME,
    policy_period_public_id STRING,
    edit_effective_date DATETIME,
    job_close_date DATETIME,
    accounting_date DATETIME,
    uw_company_code STRING,
    branch_number STRING,
    producer_code_of_record_code STRING,
    job_subtype STRING,
    financial_activity_type STRING,
    payment_plan_name STRING,
    product_code STRING,
    product_type STRING,
    product_desc STRING,
    coverable_province STRING,
    coverage_pattern_code STRING,
    class_of_business_code STRING,
    amount_category STRING,
    amount_sub_category STRING,
    amount_type STRING,
    accounting_entry_type STRING,
    gl_account_number STRING,
    actual_term_amount_amt NUMERIC,
    actual_amount_amt NUMERIC,
    gl_amount_amt NUMERIC,
    tax_rate NUMERIC,
    dpae_rate NUMERIC,
    risk_public_id STRING,
    model_number INT64,
    term_number INT64,
    cost_public_id STRING,
    cost_fixed_id STRING,
    orig_coverage_pattern_code STRING,
    external_lob_indicator STRING OPTIONS(description='External LOB indicator'),
    agnt_inspector INT64 OPTIONS(description='Agent inspector'),
    substandard_vehicle STRING OPTIONS(description='Substandard indicator'),
    risk_type STRING OPTIONS(description='Risk type'),
    brk_num STRING OPTIONS(description='Broker number'),
    old_uw_company INT64 OPTIONS(description='Old underwriter company - first digit of producer code'),
    billing_method STRING OPTIONS(description='Billing method'),
    commission_rate NUMERIC OPTIONS(description='Commission rate'),
    agnt_branch_number STRING OPTIONS(description='Branch number from agent table'),
    vehicle_type STRING OPTIONS(description='Vehicle type') ) ;