 INSERT INTO
  `@derived_project.product_gl.finance_gl_financial_activity`
SELECT
  CAST('@execution_date' AS DATETIME) AS dlh_batch_ts,
  DATETIME(TIMESTAMP(FORMAT_DATETIME("%Y-%m-%d %H:%M:%S",CURRENT_DATETIME("America/Toronto")))) AS dlh_process_ts,
  'COST' AS financial_activity_category,
  account.account_number AS policy_account_number,
  policy_period.policy_number,
  policy_period.public_id AS policy_period_public_id,
  policy_period.period_start AS policy_period_start_date,
  policy_period.period_end AS policy_period_end_date,
  policy_period.edit_effective_date AS policy_period_edit_effective_date,
  job.close_date AS job_close_date,
  all_cost.effective_date AS amount_effective_date,
  all_cost.expiration_date AS amount_expiration_date,
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
  CASE
    WHEN COALESCE(all_cost.actual_term_amount_amt, NUMERIC '0.00') > 0 THEN 'ONSET'
  ELSE
  'OFFSET'
END
  AS financial_activity_type,
  all_cost.coverage_pattern_code,
  COALESCE(all_cost.actual_amount_amt, NUMERIC '0.00') AS actual_amount_amt,
  COALESCE(all_cost.actual_term_amount_amt, NUMERIC '0.00') AS actual_term_amount_amt,
  all_cost.public_id AS cost_public_id,
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
  COALESCE(vehicle.fixed_id, '') AS vehicle_fixed_id,
  COALESCE(driver.fixed_id, '') AS driver_fixed_id,
  COALESCE(dwelling.fixed_id, '') AS dwelling_fixed_id,
  all_cost.schedule_type,
  all_cost.subtype AS cost_subtype,
  '' AS removed_cov_new_old_broker_ind,
  COALESCE(sch.fixed_id, '') AS sch_item_fixed_id,
  CAST(FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CAST(TIMESTAMP_SECONDS(UNIX_SECONDS(CURRENT_TIMESTAMP())) AS DATETIME)) AS DATETIME) AS processed_date,
  CAST(ROUND(
    IF
      (policy_period.uw_company_code <> '08', all_cost.actual_term_amount_amt - COALESCE(prior_cost.actual_term_amount_amt, NUMERIC '0.00'), all_cost.actual_term_amount_amt), 4) AS NUMERIC) AS net_term_amt,
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
 `@curated_project.gwpc.policy_period` AS policy_period
ON
  daily.public_id = policy_period.public_id
INNER JOIN
 `@curated_project.gwpc.policy` AS POLICY
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
LEFT OUTER JOIN
 `@curated_project.gwpc.account` AS account
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
      AND pp.term_number = CAST(bcpp.term_number AS INT64)
    WHERE
      DATE(bcpp.update_time) <= DATE(CAST(DATE(pp.model_date) + 1 AS DATETIME)) ) AS plan_det
  WHERE
    plan_det.plan_num = 1 ) AS pplan
ON
  pplan.policy_period_public_id = policy_period.public_id
LEFT OUTER JOIN
 `@curated_project.gwpc.job` AS job
ON
  job.policy_period_public_id = policy_period.public_id
LEFT OUTER JOIN
 `@curated_project.gwpc.producer` AS producer
ON
  producer.public_id = policy_period.producer_of_record_public_id
  AND producer.policy_period_public_id = policy_period.public_id
INNER JOIN
 `@curated_project.gwpc.all_cost` AS all_cost
ON
  all_cost.policy_period_public_id = policy_period.public_id
  AND all_cost.charge_pattern = 'Premium'
INNER JOIN
 `@curated_project.gwpc.all_coverage` AS all_coverage
ON
  all_coverage.public_id = all_cost.coverage_public_id
  AND all_coverage.pattern_code = all_cost.coverage_pattern_code
  AND all_coverage.product = all_cost.product
LEFT OUTER JOIN
 `@curated_project.gwpc.dwelling` AS dwelling
ON
  all_cost.dwelling_public_id = dwelling.public_id
  AND all_cost.policy_period_public_id = dwelling.policy_period_public_id
LEFT OUTER JOIN
 `@curated_project.gwpc.vehicle` AS vehicle
ON
  all_cost.vehicle_public_id = vehicle.public_id
  AND all_cost.policy_period_public_id = vehicle.policy_period_public_id
LEFT OUTER JOIN
 `@curated_project.gwpc.policy_contact` AS driver
ON
  all_cost.policy_period_public_id = driver.policy_period_public_id
  AND all_cost.driver_public_id = driver.public_id
LEFT OUTER JOIN
 `@curated_project.gwpc.policy_location` AS policy_location
ON
  vehicle.garage_location_public_id = policy_location.public_id
  AND policy_location.policy_period_public_id = all_coverage.policy_period_public_id
LEFT OUTER JOIN
 `@curated_project.gwpc.account_location` AS account_location
ON
  policy_location.account_location_public_id = account_location.public_id
  AND account_location.policy_period_public_id = all_coverage.policy_period_public_id
LEFT OUTER JOIN
 `@curated_project.gwpc.scheduled_item` AS sch
ON
  all_cost.scheduled_item_public_id = sch.public_id
LEFT OUTER JOIN (
  SELECT
    prior_cost_0.public_id,
    prior_cost_0.policy_period_public_id,
    prior_cost_0.coverage_pattern_code,
    prior_cost_0.subtype AS cost_subtype,
    prior_cost_0.expiration_date,
    prior_cost_0.actual_term_amount_amt,
    prior_coverage.fixed_id AS coverage_fixed_id,
    COALESCE(prior_dwelling.fixed_id, '') AS dwelling_fixed_id,
    COALESCE(prior_vehicle.fixed_id, '') AS vehicle_fixed_id,
    COALESCE(prior_driver.fixed_id, '') AS driver_fixed_id,
    COALESCE(prior_sch.fixed_id, '') AS sch_fixed_id
  FROM
   `@curated_project.gwpc.pc_daily_transaction_staging` AS daily_0
  INNER JOIN
   `@curated_project.gwpc.all_cost` AS prior_cost_0
  ON
    daily_0.public_id = prior_cost_0.policy_period_public_id
  INNER JOIN
   `@curated_project.gwpc.all_coverage` AS prior_coverage
  ON
    prior_coverage.policy_period_public_id = prior_cost_0.policy_period_public_id
    AND prior_coverage.public_id = prior_cost_0.coverage_public_id
    AND prior_coverage.pattern_code = prior_cost_0.coverage_pattern_code
    AND prior_cost_0.charge_pattern = 'Premium'
  LEFT OUTER JOIN
   `@curated_project.gwpc.dwelling` AS prior_dwelling
  ON
    prior_cost_0.dwelling_public_id = prior_dwelling.public_id
  LEFT OUTER JOIN
   `@curated_project.gwpc.vehicle` AS prior_vehicle
  ON
    prior_cost_0.vehicle_public_id = prior_vehicle.public_id
  LEFT OUTER JOIN
   `@curated_project.gwpc.policy_contact` AS prior_driver
  ON
    prior_cost_0.driver_public_id = prior_driver.public_id
  LEFT OUTER JOIN
   `@curated_project.gwpc.scheduled_item` AS prior_sch
  ON
    prior_cost_0.scheduled_item_public_id = prior_sch.public_id
  WHERE
    DATE(prior_cost_0.effective_date) <> DATE(prior_cost_0.expiration_date) ) AS prior_cost
ON
  prior_cost.policy_period_public_id = all_cost.policy_period_public_id
  AND DATE(all_cost.effective_date) = DATE(prior_cost.expiration_date)
  AND all_coverage.fixed_id = prior_cost.coverage_fixed_id
  AND all_cost.coverage_pattern_code = prior_cost.coverage_pattern_code
  AND all_cost.subtype = prior_cost.cost_subtype
  AND COALESCE(dwelling.fixed_id, '') = prior_cost.dwelling_fixed_id
  AND COALESCE(vehicle.fixed_id, '') = prior_cost.vehicle_fixed_id
  AND COALESCE(driver.fixed_id, '') = prior_cost.driver_fixed_id
  AND COALESCE(sch.fixed_id, '') = prior_cost.sch_fixed_id
  AND all_cost.public_id <> prior_cost.public_id
LEFT OUTER JOIN
 `@curated_project.gwpc.eff_dated_fields` AS eff
ON
  policy_period.public_id = eff.policy_period_public_id
WHERE
  DATE(all_cost.effective_date) <> DATE(all_cost.expiration_date)
  AND (eff.public_id IS NULL
    OR DATE(all_cost.effective_date) >= DATE(eff.effective_date)
    AND DATE(all_cost.effective_date) < DATE(eff.expiration_date))
UNION ALL
SELECT
  CAST('@execution_date' AS DATETIME) AS dlh_batch_ts,
  DATETIME(TIMESTAMP(FORMAT_DATETIME("%Y-%m-%d %H:%M:%S",CURRENT_DATETIME("America/Toronto")))) AS dlh_process_ts,
  'COST' AS financial_activity_category,
  account.account_number AS policy_account_number,
  pp.policy_number,
  pp.public_id AS policy_period_public_id,
  pp.period_start AS pp_start_date,
  pp.period_end AS pp_end_date,
  pp.edit_effective_date AS pp_edit_effective_date,
  job.close_date AS job_close_date,
  cost.expiration_date AS amount_effective_date,
  pp.period_end AS amount_expiration_date,
IF
  (DATE(pp.edit_effective_date) > DATE(job.close_date), pp.edit_effective_date, job.close_date) AS accounting_date,
  job.subtype AS job_subtype,
  CASE
    WHEN COALESCE(pplan.billing_method, pp.billing_method) = 'AgencyBill' THEN COALESCE(pplan.payment_plan_name, 'Agency Bill Annual Payment Plan')
  ELSE
  COALESCE(pplan.payment_plan_name, 'Monthly Payment Plan')
END
  AS payment_plan_name,
  pp.uw_company_code,
  pp.branch_number AS branch_number,
  COALESCE(eff.producer_code, pp.prod_cos_code) AS producer_code_of_record_code,
  policy.product_code AS product_code,
  policy.product_type AS product_type,
  policy.product_desc,
IF
  (cost.vehicle_public_id <> '', COALESCE(account_location.province, line.coverable_province, pp.province_code), COALESCE(dwelling.coverable_province, policy_line.coverable_province, line.coverable_province, pp.province_code)) AS coverable_province,
  'ONSET' AS financial_activity_type,
  cost.coverage_pattern_code,
  NUMERIC '0.00' AS actual_amount_amt,
  cost.actual_term_amount_amt AS actual_term_amount_amt,
  cost.public_id AS cost_public_id,
  COALESCE(vehicle.public_id, COALESCE(dwelling.public_id, coverage.risk_public_id)) AS risk_public_id,
  pp.model_number,
  pp.term_number,
  cost.fixed_id AS cost_fixed_id,
  coverage.pattern_code AS orig_coverage_pattern_code,
  coverage.fixed_id AS coverage_fixed_id,
  pp.external_lob_indicator,
  COALESCE(vehicle.vehicle_type,
    CASE
      WHEN policy.product_code = 'PersonalAuto' THEN 'auto'
    ELSE
    ''
  END
    ) AS vehicle_type,
  pp.branch_number AS gw_branch_number,
  NULL AS agnt_inspector,
  CAST(vehicle.substandard_vehicle AS STRING) AS substandard_vehicle,
  cost.risk_type,
  '' AS agnt_branch_number,
  COALESCE(vehicle.fixed_id, '') AS vehicle_fixed_id,
  COALESCE(driver.fixed_id, '') AS driver_fixed_id,
  COALESCE(dwelling.fixed_id, '') AS dwelling_fixed_id,
  cost.schedule_type,
  cost.subtype AS cost_subtype,
  '' AS removed_cov_new_old_broker_ind,
  COALESCE(sch.fixed_id, '') AS sch_item_fixed_id,
  CAST(FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CAST(TIMESTAMP_SECONDS(UNIX_SECONDS(CURRENT_TIMESTAMP())) AS DATETIME)) AS DATETIME) AS processed_date,
  CAST(cost.actual_term_amount_amt * -1 AS NUMERIC) AS net_term_amt,
  FALSE AS eot,
  EXTRACT(YEAR
  FROM
    CAST(pp.processed_date AS DATETIME)) AS tran_proc_year,
  EXTRACT(MONTH
  FROM
    CAST(pp.processed_date AS DATETIME)) AS tran_proc_month
FROM
 `@curated_project.gwpc.pc_daily_transaction_staging` AS daily
INNER JOIN
 `@curated_project.gwpc.policy_period` AS pp
ON
  daily.public_id = pp.public_id
INNER JOIN
 `@curated_project.gwpc.all_cost` AS cost
ON
  pp.public_id = cost.policy_period_public_id
  AND pp.uw_company_code <> '08'
  AND ABS(cost.actual_term_amount_amt) > 0
  AND cost.subtype NOT IN( 'PersonalAutoShortRateCovCost_ECO',
    'PersonalVehicleShortRateCovCost_ECO',
    'HOShortRatePenaltyCost_ECO',
    'HomeownersShortRateCovCost_ECO',
    'DwellingShortRateCovCost_ECO',
    'ScheduleByTypeCovShortRateCost_ECO',
    'ScheduleCovShortRateCost_ECO' )
INNER JOIN
 `@curated_project.gwpc.all_coverage` AS coverage
ON
  coverage.policy_period_public_id = cost.policy_period_public_id
  AND coverage.public_id = cost.coverage_public_id
  AND coverage.pattern_code = cost.coverage_pattern_code
  AND cost.charge_pattern = 'Premium'
LEFT OUTER JOIN
 `@curated_project.gwpc.dwelling` AS dwelling
ON
  cost.dwelling_public_id = dwelling.public_id
LEFT OUTER JOIN
 `@curated_project.gwpc.vehicle` AS vehicle
ON
  cost.vehicle_public_id = vehicle.public_id
LEFT OUTER JOIN
 `@curated_project.gwpc.policy_contact` AS driver
ON
  cost.driver_public_id = driver.public_id
LEFT OUTER JOIN
 `@curated_project.gwpc.scheduled_item` AS sch
ON
  cost.scheduled_item_public_id = sch.public_id
LEFT OUTER JOIN (
  SELECT
    cost_0.public_id,
    cost_0.policy_period_public_id,
    cost_0.coverage_pattern_code,
    cost_0.subtype AS cost_subtype,
    coverage_0.fixed_id AS coverage_fixed_id,
    COALESCE(dwelling_0.fixed_id, '') AS dwelling_fixed_id,
    COALESCE(vehicle_0.fixed_id, '') AS vehicle_fixed_id,
    COALESCE(driver_0.fixed_id, '') AS driver_fixed_id,
    COALESCE(sch_0.fixed_id, '') AS sch_fixed_id,
    DATE(cost_0.effective_date) AS effective_date
  FROM
   `@curated_project.gwpc.pc_daily_transaction_staging` AS daily_0
  INNER JOIN
   `@curated_project.gwpc.all_cost` AS cost_0
  ON
    daily_0.public_id = cost_0.policy_period_public_id
  INNER JOIN
   `@curated_project.gwpc.all_coverage` AS coverage_0
  ON
    coverage_0.policy_period_public_id = cost_0.policy_period_public_id
    AND coverage_0.public_id = cost_0.coverage_public_id
    AND coverage_0.pattern_code = cost_0.coverage_pattern_code
    AND cost_0.charge_pattern = 'Premium'
  LEFT OUTER JOIN
   `@curated_project.gwpc.dwelling` AS dwelling_0
  ON
    cost_0.dwelling_public_id = dwelling_0.public_id
  LEFT OUTER JOIN
   `@curated_project.gwpc.vehicle` AS vehicle_0
  ON
    cost_0.vehicle_public_id = vehicle_0.public_id
  LEFT OUTER JOIN
   `@curated_project.gwpc.policy_contact` AS driver_0
  ON
    cost_0.driver_public_id = driver_0.public_id
  LEFT OUTER JOIN
   `@curated_project.gwpc.scheduled_item` AS sch_0
  ON
    cost_0.scheduled_item_public_id = sch_0.public_id
  WHERE
    DATE(cost_0.effective_date) <> DATE(cost_0.expiration_date) ) AS nxt_cost
ON
  nxt_cost.policy_period_public_id = cost.policy_period_public_id
  AND cost.coverage_pattern_code = nxt_cost.coverage_pattern_code
  AND coverage.fixed_id = nxt_cost.coverage_fixed_id
  AND cost.subtype = nxt_cost.cost_subtype
  AND COALESCE(dwelling.fixed_id, '') = nxt_cost.dwelling_fixed_id
  AND COALESCE(vehicle.fixed_id, '') = nxt_cost.vehicle_fixed_id
  AND COALESCE(driver.fixed_id, '') = nxt_cost.driver_fixed_id
  AND COALESCE(sch.fixed_id, '') = nxt_cost.sch_fixed_id
  AND DATE(cost.expiration_date) = nxt_cost.effective_date
LEFT OUTER JOIN
 `@curated_project.gwpc.eff_dated_fields` AS eff
ON
  pp.public_id = eff.policy_period_public_id
INNER JOIN
 `@curated_project.gwpc.job` AS job
ON
  pp.public_id = job.policy_period_public_id
INNER JOIN
 `@curated_project.gwpc.policy` AS POLICY
ON
  policy.policy_period_public_id = pp.public_id
INNER JOIN
 `@curated_project.gwpc.policy_line` AS line
ON
  pp.line_public_id = line.public_id
LEFT OUTER JOIN (
  SELECT
    dwl.policy_period_public_id,
    dwl.coverable_province
  FROM
   `@curated_project.gwpc.pc_daily_transaction_staging` AS dlytrn1
  INNER JOIN
   `@curated_project.gwpc.policy_period` AS pp_0
  ON
    dlytrn1.public_id = pp_0.public_id
  INNER JOIN
   `@curated_project.gwpc.dwelling` AS dwl
  ON
    dwl.policy_period_public_id = pp_0.public_id
    AND dwl.primary_dwelling = 'true'
  WHERE
    DATE(pp_0.edit_effective_date) >= DATE(dwl.effective_date)
    AND DATE(pp_0.edit_effective_date) < DATE(dwl.expiration_date) ) AS policy_line
ON
  policy_line.policy_period_public_id = pp.public_id
LEFT OUTER JOIN
 `@curated_project.gwpc.account` AS account
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
      pp_0.public_id AS policy_period_public_id,
      COALESCE(bcpp.billing_method, pp_0.billing_method) AS billing_method,
      CASE
        WHEN COALESCE(bcpp.billing_method, pp_0.billing_method) = 'AgencyBill' THEN COALESCE(bcpp.payment_plan_name, 'Agency Bill Annual Payment Plan')
      ELSE
      COALESCE(bcpp.payment_plan_name, 'Monthly Payment Plan')
    END
      AS payment_plan_name,
      ROW_NUMBER() OVER (PARTITION BY pp_0.public_id ORDER BY bcpp.edh_bc_sequence_id DESC) AS plan_num
    FROM
     `@curated_project.gwpc.pc_daily_transaction_staging` AS dlytrn1
    INNER JOIN
     `@curated_project.gwpc.policy_period` AS pp_0
    ON
      dlytrn1.public_id = pp_0.public_id
    INNER JOIN
     `@curated_project.gwbc.policy_period` AS bcpp
    ON
      pp_0.policy_number = bcpp.policy_number
      AND pp_0.term_number = CAST(bcpp.term_number AS INT64)
    WHERE
      DATE(bcpp.update_time) <= DATE(CAST(DATE(pp_0.model_date) + 1 AS DATETIME)) ) AS plan_det
  WHERE
    plan_det.plan_num = 1 ) AS pplan
ON
  pplan.policy_period_public_id = pp.public_id
LEFT OUTER JOIN
 `@curated_project.gwpc.producer` AS producer
ON
  producer.public_id = pp.producer_of_record_public_id
  AND producer.policy_period_public_id = pp.public_id
LEFT OUTER JOIN
 `@curated_project.gwpc.policy_location` AS policy_location
ON
  vehicle.garage_location_public_id = policy_location.public_id
  AND policy_location.policy_period_public_id = coverage.policy_period_public_id
LEFT OUTER JOIN
 `@curated_project.gwpc.account_location` AS account_location
ON
  policy_location.account_location_public_id = account_location.public_id
  AND account_location.policy_period_public_id = coverage.policy_period_public_id
WHERE
  DATE(cost.effective_date) <> DATE(cost.expiration_date)
  AND DATE(cost.expiration_date) <> DATE(pp.period_end)
  AND nxt_cost.policy_period_public_id IS NULL
  AND (eff.public_id IS NULL
    OR DATE(cost.expiration_date) >= DATE(eff.effective_date)
    AND DATE(cost.expiration_date) < DATE(eff.expiration_date)) ;