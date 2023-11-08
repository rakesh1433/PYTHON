 INSERT INTO
  `@derived_project.product_gl.finance_gl_financial_activity`
SELECT
  CAST('@execution_date' AS DATETIME) AS dlh_batch_ts,
  DATETIME(TIMESTAMP(FORMAT_DATETIME("%Y-%m-%d %H:%M:%S",CURRENT_DATETIME("America/Toronto")))) AS dlh_process_ts,
  'TRANSACTION' AS financial_activity_category,
  account.account_number AS policy_account_number,
  policy_period.policy_number,
  policy_period.public_id AS policy_period_public_id,
  policy_period.period_start AS policy_period_start_date,
  policy_period.period_end AS policy_period_end_date,
  policy_period.edit_effective_date AS policy_period_edit_effective_date,
  job.close_date AS job_close_date,
  pc_transaction.effective_date AS amount_effective_date,
  pc_transaction.expiration_date AS amount_expiration_date,
IF
  (policy_period.edit_effective_date > job.close_date, policy_period.edit_effective_date, job.close_date) AS accounting_date,
  job.subtype AS job_subtype,
  CASE
    WHEN COALESCE(pplan.billing_method, policy_period.billing_method) = 'AgencyBill' THEN COALESCE(pplan.payment_plan_name, 'Agency Bill Annual Payment Plan')
  ELSE
  COALESCE(pplan.payment_plan_name, 'Monthly Payment Plan')
END
  AS payment_plan_name,
  policy_period.uw_company_code,
  policy_period.branch_number AS branch_number,
  COALESCE(eff.producer_code, policy_period.prod_cos_code) AS producer_code_of_record_code,
  policy.product_code,
  policy.product_type,
  policy.product_desc,
IF
  (all_cost.vehicle_public_id <> '', COALESCE(account_location.province, line.coverable_province, policy_period.province_code), COALESCE(dwelling.coverable_province, policy_line.coverable_province, line.coverable_province, policy_period.province_code)) AS coverable_province,
IF
  (COALESCE(pc_transaction.amount_amt, NUMERIC '0.00') > 0, 'ONSET', 'OFFSET') AS financial_activity_type,
  all_coverage.pattern_code AS coverage_pattern_code,
  COALESCE(pc_transaction.amount_amt, NUMERIC '0.00') AS actual_amount_amt,
  COALESCE(all_cost.actual_term_amount_amt, NUMERIC '0.00') AS actual_term_amount_amt,
  pc_transaction.all_cost_public_id AS cost_public_id,
  COALESCE(vehicle.public_id, COALESCE(dwelling.public_id, all_coverage.risk_public_id)) AS risk_public_id,
  policy_period.model_number,
  policy_period.term_number,
  all_cost.fixed_id AS cost_fixed_id,
  all_coverage.pattern_code AS orig_coverage_pattern_code,
  all_coverage.fixed_id AS coverage_fixed_id,
  policy_period.external_lob_indicator,
  COALESCE(vehicle.vehicle_type,
    CASE
      WHEN policy.product_code = 'PersonalAuto' THEN 'auto'
    ELSE
    ''
  END
    ) AS vehicle_type,
  policy_period.branch_number AS gw_branch_number,
  NULL AS agnt_inspector,
  CAST(vehicle.substandard_vehicle AS STRING) AS substandard_vehicle,
  all_cost.risk_type,
  '' AS agnt_branch_number,
  vehicle.fixed_id AS vehicle_fixed_id,
  driver.fixed_id AS driver_fixed_id,
  dwelling.fixed_id AS dwelling_fixed_id,
  all_cost.schedule_type,
  all_cost.subtype AS cost_subtype,
  '' AS removed_cov_new_old_broker_ind,
  COALESCE(sch.fixed_id, '') AS sch_item_fixed_id,
  CAST(FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CAST(TIMESTAMP_SECONDS(UNIX_SECONDS(CURRENT_TIMESTAMP())) AS DATETIME)) AS DATETIME) AS processed_date,
  ROUND(NUMERIC '0.00', 4) AS net_term_amt,
  CASE
    WHEN job.subtype = 'Cancellation' OR job.close_date > policy_period.period_end THEN TRUE
  ELSE
  FALSE
END
  AS eot,
  EXTRACT(YEAR
  FROM
    CAST(policy_period.processed_date AS DATETIME)) AS tran_proc_year,
  EXTRACT(MONTH
  FROM
    CAST(policy_period.processed_date AS DATETIME)) AS tran_proc_month
FROM
  `@curated_project.gwpc.pc_daily_transaction_staging` AS daily
INNER JOIN
  `@curated_project.gwpc.policy_period` policy_period
ON
  daily.public_id = policy_period.public_id
INNER JOIN
  `@curated_project.gwpc.policy` POLICY
ON
  policy.policy_period_public_id = policy_period.public_id
INNER JOIN
  `@curated_project.gwpc.policy_line` AS line
ON
  policy_period.line_public_id = line.public_id
LEFT OUTER JOIN
  `@curated_project.gwpc.account` account
ON
  account.public_id = policy.account_public_id
  AND account.policy_period_public_id = policy.policy_period_public_id
LEFT OUTER JOIN (
  SELECT
    plan_det.policy_period_public_id,
    plan_det.payment_plan_name,
    plan_det.billing_method
  FROM (
    SELECT
      pp.public_id AS policy_period_public_id,
      COALESCE(bcpp.billing_method, pp.billing_method) AS billing_method,
      CASE
        WHEN COALESCE(bcpp.billing_method, pp.billing_method) = 'AgencyBill' THEN COALESCE(bcpp.payment_plan_name, 'Agency Bill Annual Payment Plan')
      ELSE
      COALESCE(bcpp.payment_plan_name, 'Monthly Payment Plan')
    END
      AS payment_plan_name,
      ROW_NUMBER() OVER (PARTITION BY pp.public_id ORDER BY bcpp.edh_bc_sequence_id DESC) AS plan_num
    FROM
      `@curated_project.gwpc.pc_daily_transaction_staging` AS dlytrn1
    INNER JOIN
      `@curated_project.gwpc.policy_period` AS pp
    ON
      dlytrn1.public_id = pp.public_id
    INNER JOIN
      `@curated_project.gwbc.policy_period` AS bcpp
    ON
      pp.policy_number = bcpp.policy_number
      AND CAST(pp.term_number AS STRING) = bcpp.term_number
    WHERE
      DATE(bcpp.update_time) <= DATE(CAST(DATE(pp.model_date) + 1 AS DATETIME)) ) AS plan_det
  WHERE
    plan_det.plan_num = 1 ) AS pplan
ON
  pplan.policy_period_public_id = policy_period.public_id
LEFT OUTER JOIN
  `@curated_project.gwpc.job` job
ON
  job.policy_period_public_id = policy_period.public_id
LEFT OUTER JOIN
  `@curated_project.gwpc.producer` producer
ON
  producer.public_id = policy_period.producer_of_record_public_id
  AND producer.policy_period_public_id = policy_period.public_id
LEFT OUTER JOIN (
  SELECT
    dwl.policy_period_public_id,
    dwl.coverable_province
  FROM
    `@curated_project.gwpc.pc_daily_transaction_staging` AS dlytrn1
  INNER JOIN
    `@curated_project.gwpc.policy_period` AS pp
  ON
    dlytrn1.public_id = pp.public_id
  INNER JOIN
    `@curated_project.gwpc.dwelling` AS dwl
  ON
    dwl.policy_period_public_id = pp.public_id
    AND dwl.primary_dwelling = 'true'
  WHERE
    DATE(pp.edit_effective_date) >= DATE(dwl.effective_date)
    AND DATE(pp.edit_effective_date) < DATE(dwl.expiration_date) ) AS policy_line
ON
  policy_line.policy_period_public_id = policy_period.public_id
LEFT OUTER JOIN
  `@curated_project.gwpc.transaction` AS pc_transaction
ON
  pc_transaction.policy_period_public_id = policy_period.public_id
INNER JOIN
  `@curated_project.gwpc.all_cost` all_cost
ON
  all_cost.public_id = pc_transaction.all_cost_public_id
  AND all_cost.product = pc_transaction.product
  AND all_cost.charge_pattern = 'Premium'
INNER JOIN
  `@curated_project.gwpc.all_coverage` all_coverage
ON
  all_coverage.public_id = all_cost.coverage_public_id
  AND all_coverage.pattern_code = all_cost.coverage_pattern_code
  AND all_coverage.product = policy.product_code
INNER JOIN
  `@curated_project.gwpc.policy_period` AS cost_policy_period
ON
  all_cost.policy_period_public_id = cost_policy_period.public_id
LEFT OUTER JOIN
  `@curated_project.gwpc.dwelling` dwelling
ON
  all_cost.dwelling_public_id = dwelling.public_id
  AND dwelling.policy_period_public_id = all_coverage.policy_period_public_id
LEFT OUTER JOIN
  `@curated_project.gwpc.vehicle` vehicle
ON
  all_cost.vehicle_public_id = vehicle.public_id
  AND vehicle.policy_period_public_id = all_coverage.policy_period_public_id
LEFT OUTER JOIN
  `@curated_project.gwpc.policy_contact` AS driver
ON
  all_cost.policy_period_public_id = driver.policy_period_public_id
  AND all_cost.driver_public_id = driver.public_id
LEFT OUTER JOIN
  `@curated_project.gwpc.policy_location` policy_location
ON
  vehicle.garage_location_public_id = policy_location.public_id
  AND policy_location.policy_period_public_id = all_coverage.policy_period_public_id
LEFT OUTER JOIN
  `@curated_project.gwpc.account_location` account_location
ON
  policy_location.account_location_public_id = account_location.public_id
  AND account_location.policy_period_public_id = all_coverage.policy_period_public_id
LEFT OUTER JOIN
  `@curated_project.gwpc.scheduled_item` AS sch
ON
  all_cost.scheduled_item_public_id = sch.public_id
LEFT OUTER JOIN
  `@curated_project.gwpc.eff_dated_fields` AS eff
ON
  policy_period.public_id = eff.policy_period_public_id
WHERE
  eff.policy_period_public_id IS NULL
  OR DATE(policy_period.edit_effective_date) >= DATE(eff.effective_date)
  AND DATE(policy_period.edit_effective_date) < DATE(eff.expiration_date)
UNION ALL
SELECT
  CAST('@execution_date' AS DATETIME) AS dlh_batch_ts,
  DATETIME(TIMESTAMP(FORMAT_DATETIME("%Y-%m-%d %H:%M:%S",CURRENT_DATETIME("America/Toronto")))) AS dlh_process_ts,
  'TRANSACTION' AS financial_activity_category,
  account.account_number AS policy_account_number,
  policy_period.policy_number,
  policy_period.public_id AS policy_period_public_id,
  policy_period.period_start AS policy_period_start_date,
  policy_period.period_end AS policy_period_end_date,
  policy_period.edit_effective_date AS policy_period_edit_effective_date,
  job.close_date AS job_close_date,
  pc_transaction.effective_date AS amount_effective_date,
  pc_transaction.expiration_date AS amount_expiration_date,
IF
  (policy_period.edit_effective_date > job.close_date, policy_period.edit_effective_date, job.close_date) AS accounting_date,
  job.subtype AS job_subtype,
  CASE
    WHEN COALESCE(pplan.billing_method, policy_period.billing_method) = 'AgencyBill' THEN COALESCE(pplan.payment_plan_name, 'Agency Bill Annual Payment Plan')
  ELSE
  COALESCE(pplan.payment_plan_name, 'Monthly Payment Plan')
END
  AS payment_plan_name,
  policy_period.uw_company_code,
  policy_period.branch_number AS branch_number,
  COALESCE(eff.producer_code, policy_period.prod_cos_code) AS producer_code_of_record_code,
  policy.product_code,
  policy.product_type,
  policy.product_desc,
IF
  (all_cost.vehicle_public_id <> '', COALESCE(account_location.province, line.coverable_province, policy_period.province_code), COALESCE(dwelling.coverable_province, policy_line.coverable_province, line.coverable_province, policy_period.province_code)) AS coverable_province,
IF
  (COALESCE(pc_transaction.amount_amt, NUMERIC '0.00') > 0, 'ONSET', 'OFFSET') AS financial_activity_type,
  'ALL_COVERAGES' AS coverage_pattern_code,
  COALESCE(pc_transaction.amount_amt, NUMERIC '0.00') AS actual_amount_amt,
  NUMERIC '0.00' AS actual_term_amount_amt,
  pc_transaction.all_cost_public_id AS cost_public_id,
  COALESCE(vehicle.public_id, COALESCE(dwelling.public_id, all_coverage.risk_public_id)) AS risk_public_id,
  policy_period.model_number,
  policy_period.term_number,
  all_cost.fixed_id AS cost_fixed_id,
  all_coverage.pattern_code AS orig_coverage_pattern_code,
  all_coverage.fixed_id AS coverage_fixed_id,
  policy_period.external_lob_indicator,
  COALESCE(vehicle.vehicle_type,
    CASE
      WHEN policy.product_code = 'PersonalAuto' THEN 'auto'
    ELSE
    ''
  END
    ) AS vehicle_type,
  policy_period.branch_number AS gw_branch_number,
  NULL AS agnt_inspector,
  CAST(vehicle.substandard_vehicle AS STRING) AS substandard_vehicle,
  all_cost.risk_type,
  '' AS agnt_branch_number,
  vehicle.fixed_id AS vehicle_fixed_id,
  driver.fixed_id AS driver_fixed_id,
  dwelling.fixed_id AS dwelling_fixed_id,
  all_cost.schedule_type,
  all_cost.subtype AS cost_subtype,
  '' AS removed_cov_new_old_broker_ind,
  COALESCE(sch.fixed_id, '') AS sch_item_fixed_id,
  CAST(FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CAST(TIMESTAMP_SECONDS(UNIX_SECONDS(CURRENT_TIMESTAMP())) AS DATETIME)) AS DATETIME) AS processed_date,
  ROUND(NUMERIC '0.00', 4) AS net_term_amt,
  FALSE AS eot,
  EXTRACT(YEAR
  FROM
    CAST(policy_period.processed_date AS DATETIME)) AS tran_proc_year,
  EXTRACT(MONTH
  FROM
    CAST(policy_period.processed_date AS DATETIME)) AS tran_proc_month
FROM
  `@curated_project.gwpc.pc_daily_transaction_staging` AS daily
INNER JOIN
  `@curated_project.gwpc.policy_period` policy_period
ON
  daily.public_id = policy_period.public_id
INNER JOIN
  `@curated_project.gwpc.policy` POLICY
ON
  policy.policy_period_public_id = policy_period.public_id
INNER JOIN
  `@curated_project.gwpc.policy_line` AS line
ON
  policy_period.line_public_id = line.public_id
LEFT OUTER JOIN (
  SELECT
    dwl.policy_period_public_id,
    dwl.coverable_province
  FROM
    `@curated_project.gwpc.pc_daily_transaction_staging` AS dlytrn1
  INNER JOIN
    `@curated_project.gwpc.policy_period` AS pp
  ON
    dlytrn1.public_id = pp.public_id
  INNER JOIN
    `@curated_project.gwpc.dwelling` AS dwl
  ON
    dwl.policy_period_public_id = pp.public_id
    AND dwl.primary_dwelling = 'true'
  WHERE
    DATE(pp.edit_effective_date) >= DATE(dwl.effective_date)
    AND DATE(pp.edit_effective_date) < DATE(dwl.expiration_date) ) AS policy_line
ON
  policy_line.policy_period_public_id = policy_period.public_id
LEFT OUTER JOIN (
  SELECT
    plan_det.policy_period_public_id,
    plan_det.payment_plan_name,
    plan_det.billing_method
  FROM (
    SELECT
      pp.public_id AS policy_period_public_id,
      COALESCE(bcpp.billing_method, pp.billing_method) AS billing_method,
      CASE
        WHEN COALESCE(bcpp.billing_method, pp.billing_method) = 'AgencyBill' THEN COALESCE(bcpp.payment_plan_name, 'Agency Bill Annual Payment Plan')
      ELSE
      COALESCE(bcpp.payment_plan_name, 'Monthly Payment Plan')
    END
      AS payment_plan_name,
      ROW_NUMBER() OVER (PARTITION BY pp.public_id ORDER BY bcpp.edh_bc_sequence_id DESC) AS plan_num
    FROM
      `@curated_project.gwpc.pc_daily_transaction_staging` AS dlytrn1
    INNER JOIN
      `@curated_project.gwpc.policy_period` AS pp
    ON
      dlytrn1.public_id = pp.public_id
    INNER JOIN
      `@curated_project.gwbc.policy_period` AS bcpp
    ON
      pp.policy_number = bcpp.policy_number
      AND CAST(pp.term_number AS STRING) = bcpp.term_number
    WHERE
      DATE(bcpp.update_time) <= DATE(CAST(DATE(pp.model_date) + 1 AS DATETIME)) ) AS plan_det
  WHERE
    plan_det.plan_num = 1 ) AS pplan
ON
  pplan.policy_period_public_id = policy_period.public_id
LEFT OUTER JOIN
  `@curated_project.gwpc.account` account
ON
  account.public_id = policy.account_public_id
  AND account.policy_period_public_id = policy.policy_period_public_id
LEFT OUTER JOIN
  `@curated_project.gwpc.job` job
ON
  job.policy_period_public_id = policy_period.public_id
LEFT OUTER JOIN
  `@curated_project.gwpc.producer` producer
ON
  producer.public_id = policy_period.producer_of_record_public_id
  AND producer.policy_period_public_id = policy_period.public_id
LEFT OUTER JOIN
  `@curated_project.gwpc.transaction` AS pc_transaction
ON
  pc_transaction.policy_period_public_id = policy_period.public_id
INNER JOIN
  `@curated_project.gwpc.all_cost` all_cost
ON
  all_cost.public_id = pc_transaction.all_cost_public_id
  AND all_cost.product = pc_transaction.product
  AND all_cost.charge_pattern = 'Premium'
INNER JOIN
  `@curated_project.gwpc.policy_period` AS cost_policy_period
ON
  all_cost.policy_period_public_id = cost_policy_period.public_id
INNER JOIN
  `@curated_project.gwpc.all_coverage` all_coverage
ON
  all_coverage.public_id = all_cost.coverage_public_id
  AND all_coverage.pattern_code = all_cost.coverage_pattern_code
  AND all_coverage.product = policy.product_code
LEFT OUTER JOIN
  `@curated_project.gwpc.dwelling` dwelling
ON
  all_cost.dwelling_public_id = dwelling.public_id
  AND dwelling.policy_period_public_id = all_coverage.policy_period_public_id
LEFT OUTER JOIN
  `@curated_project.gwpc.vehicle` vehicle
ON
  all_cost.vehicle_public_id = vehicle.public_id
  AND vehicle.policy_period_public_id = all_coverage.policy_period_public_id
LEFT OUTER JOIN
  `@curated_project.gwpc.policy_contact` AS driver
ON
  all_cost.policy_period_public_id = driver.policy_period_public_id
  AND all_cost.driver_public_id = driver.public_id
LEFT OUTER JOIN
  `@curated_project.gwpc.policy_location` policy_location
ON
  vehicle.garage_location_public_id = policy_location.public_id
  AND policy_location.policy_period_public_id = all_coverage.policy_period_public_id
LEFT OUTER JOIN
  `@curated_project.gwpc.account_location` account_location
ON
  policy_location.account_location_public_id = account_location.public_id
  AND account_location.policy_period_public_id = all_coverage.policy_period_public_id
LEFT OUTER JOIN
  `@curated_project.gwpc.scheduled_item` AS sch
ON
  all_cost.scheduled_item_public_id = sch.public_id
LEFT OUTER JOIN
  `@curated_project.gwpc.eff_dated_fields` AS eff
ON
  policy_period.public_id = eff.policy_period_public_id
WHERE
  pc_transaction.amount_amt <> 0
  AND (eff.policy_period_public_id IS NULL
    OR DATE(policy_period.edit_effective_date) >= DATE(eff.effective_date)
    AND DATE(policy_period.edit_effective_date) < DATE(eff.expiration_date)) ;