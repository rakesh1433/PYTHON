-- comment

 INSERT INTO
  `@derived_project.product_gl.finance_gl_pc_detail`
SELECT
  CAST('@execution_date' AS DATETIME) AS dlh_batch_ts,
  DATETIME(TIMESTAMP(FORMAT_DATETIME("%Y-%m-%dT%H:%M:%S",CURRENT_DATETIME("America/Toronto")))) AS dlh_process_ts,
  finca.policy_account_number,
  finca.policy_number,
  finca.policy_period_start_date,
  finca.policy_period_end_date,
  finca.policy_period_public_id,
  finca.edit_effective_date,
  finca.job_close_date,
  finca.accounting_date,
  finca.uw_company_code,
  CASE finca.uw_company_code
    WHEN '08' THEN finca.branch_number
  ELSE
  COALESCE(branch.gl_branch_number, COALESCE(CAST(agnt.branch_number AS STRING), finca.branch_number))
END
  AS branch_number,
  finca.producer_code_of_record_code,
  finca.job_subtype,
  CASE
    WHEN finca.net_term_amt > 0 THEN 'ONSET'
  ELSE
  'OFFSET'
END
  AS financial_activity_type,
  CASE
    WHEN COALESCE(chg.billing_method, finca.billing_method) = 'AgencyBill' THEN COALESCE(finca.payment_plan_name, 'Agency Bill Annual Payment Plan')
  ELSE
  COALESCE(finca.payment_plan_name, 'Monthly Payment Plan')
END
  AS payment_plan_name,
  finca.product_code,
  finca.product_type,
  finca.product_desc,
  finca.coverable_province,
  finca.coverage_pattern_code,
  gl_clob.class_of_business_code AS class_of_business_code,
  'UNEARNED PREMIUM' AS amount_category,
  accounting_rule.amount_sub_category AS amount_sub_category,
  accounting_rule.amount_type AS amount_type,
  accounting_rule.accounting_entry_type AS accounting_entry_type,
  accounting_rule.gl_account_number AS gl_account_number,
  CAST(finca.net_term_amt AS NUMERIC) AS actual_term_amount_amt,
  CAST(finca.net_term_amt AS NUMERIC) AS actual_amount_amt,
  CAST(ROUND(CASE accounting_rule.amount_type
        WHEN 'EARNED PREMIUMS' THEN ROUND(COALESCE(CAST(ABS(finca.net_term_amt) AS BIGNUMERIC), NUMERIC '0.00') / 365 * finca.num_days, 4)
        WHEN 'EARNED TAXES' THEN ROUND(COALESCE(CAST(ABS(finca.net_term_amt) AS BIGNUMERIC), NUMERIC '0.00') / 365 * finca.num_days, 4) * (COALESCE(premium_tax.premium_tax_rate, NUMERIC '0.0000') + COALESCE(premium_tax.fire_tax_rate, NUMERIC '0.0000'))
        WHEN 'EARNED DPAE' THEN ROUND(COALESCE(CAST(ABS(finca.net_term_amt) AS BIGNUMERIC), NUMERIC '0.00') / 365 * finca.num_days, 4) * COALESCE(dpae.dpae_rate, NUMERIC '0.0000')
      ELSE
      NUMERIC '0.00'
    END
      , 4) AS NUMERIC) AS gl_amount_amt,
  CAST(ROUND(
    IF
      (accounting_rule.amount_type IN( 'EARNED TAXES' ), COALESCE(premium_tax.premium_tax_rate, NUMERIC '0.0000') + COALESCE(premium_tax.fire_tax_rate, NUMERIC '0.0000'), NUMERIC '0.0000'), 4) AS NUMERIC) AS tax_rate,
  CAST(ROUND(
    IF
      (accounting_rule.amount_type IN( 'EARNED DPAE' ), COALESCE(dpae.dpae_rate, NUMERIC '0.0000'), NUMERIC '0.0000'), 4) AS NUMERIC) AS dpae_rate,
  finca.risk_public_id,
  finca.model_number,
  finca.term_number,
  finca.cost_public_id,
  finca.cost_fixed_id,
  finca.orig_coverage_pattern_code,
  finca.external_lob_indicator,
  agnt.insp_num AS agnt_inspector,
  finca.substandard_vehicle,
  finca.risk_type,
  SUBSTR(COALESCE(agnt.gw_brk_num, chg.prim_pol_comm_producer_code), 3) AS brk_num,
  CAST(SUBSTR(COALESCE(agnt.gw_brk_num, chg.prim_pol_comm_producer_code), 1, 1) AS INT64) AS old_uw_company,
  COALESCE(chg.billing_method, finca.billing_method) AS billing_method,
  chg.charge_commission_rate AS commission_rate,
  CAST(agnt.branch_number AS STRING) AS agnt_branch_number,
  finca.vehicle_type,
  finca.job_source,
  CAST(FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CAST(TIMESTAMP_SECONDS(UNIX_SECONDS(CURRENT_TIMESTAMP())) AS DATETIME)) AS DATETIME) AS processed_date,
  EXTRACT(YEAR
  FROM
    CAST(finca.accounting_date AS DATETIME)) AS acc_year,
  EXTRACT(MONTH
  FROM
    CAST(finca.accounting_date AS DATETIME)) AS acc_month,
  EXTRACT(DAY
  FROM
    CAST(finca.accounting_date AS DATETIME)) AS acc_day
FROM (
  SELECT
    finca_0.policy_account_number,
    finca_0.policy_number,
    finca_0.policy_period_start_date,
    finca_0.policy_period_end_date,
    trns.policy_period_public_id,
    trns.edit_effective_date,
    trns.job_close_date,
    trns.job_close_date AS accounting_date,
    finca_0.uw_company_code,
    finca_0.branch_number,
    COALESCE(eff.producer_code, trns.broker_code) AS producer_code_of_record_code,
    trns.job_subtype,
    finca_0.payment_plan_name,
    finca_0.product_code,
    finca_0.product_type,
    finca_0.product_desc,
    finca_0.coverable_province,
    finca_0.coverage_pattern_code,
    finca_0.net_term_amt * -1 AS net_term_amt,
    finca_0.risk_public_id,
    finca_0.model_number,
    finca_0.term_number,
    finca_0.cost_public_id,
    finca_0.cost_fixed_id,
    finca_0.orig_coverage_pattern_code,
    finca_0.external_lob_indicator,
    finca_0.agnt_inspector,
    finca_0.substandard_vehicle,
    finca_0.risk_type,
    finca_0.agnt_branch_number,
    finca_0.vehicle_type,
    'rev_bckdt_earnings' AS job_source,
    billing_pp.billing_method,
    DATE_DIFF(LEAST(
      IF
        (trns.uw_company_code = '08', DATE(finca_0.amount_expiration_date), DATE(trns.model_date)), DATE(trns.model_date), DATE(trns.period_end)), GREATEST(DATE(finca_0.amount_effective_date), DATE(trns.edit_effective_date)), DAY) AS num_days
  FROM
    `@derived_project.product_gl.bckdtd_trans_staging` AS trns
  INNER JOIN
    `@derived_project.product_gl.finance_gl_financial_activity` AS finca_0
  ON
    finca_0.policy_period_public_id = trns.prior_policy_period_public_id
    AND finca_0.financial_activity_category = 'COST'
    AND finca_0.removed_cov_new_old_broker_ind = ''
    AND finca_0.cost_subtype NOT IN( 'PersonalAutoShortRateCovCost_ECO',
      'PersonalVehicleShortRateCovCost_ECO',
      'HOShortRatePenaltyCost_ECO',
      'HomeownersShortRateCovCost_ECO',
      'DwellingShortRateCovCost_ECO',
      'ScheduleByTypeCovShortRateCost_ECO',
      'ScheduleCovShortRateCost_ECO' )
  INNER JOIN
    `@curated_project.gwpc.policy_period` AS billing_pp
  ON
    billing_pp.public_id = finca_0.policy_period_public_id
  LEFT OUTER JOIN
    `@curated_project.gwpc.eff_dated_fields` AS eff
  ON
    trns.prior_policy_period_public_id = eff.policy_period_public_id
  LEFT OUTER JOIN (
    SELECT
      DISTINCT trns_0.policy_period_public_id,
      finca_1.coverage_pattern_code,
      finca_1.coverage_fixed_id,
      finca_1.vehicle_fixed_id,
      finca_1.driver_fixed_id,
      finca_1.dwelling_fixed_id,
      finca_1.cost_subtype,
      finca_1.sch_item_fixed_id
    FROM
      `@derived_project.product_gl.bckdtd_trans_staging` AS dly
    INNER JOIN
      `@curated_project.gwpc.transaction` AS trns_0
    ON
      dly.policy_period_public_id = trns_0.policy_period_public_id
    INNER JOIN
      `@derived_project.product_gl.finance_gl_financial_activity` AS finca_1
    ON
      finca_1.policy_period_public_id = trns_0.policy_period_public_id
      AND finca_1.financial_activity_category = 'TRANSACTION'
      AND finca_1.coverage_pattern_code <> 'ALL_COVERAGES' ) AS ct
  ON
    trns.policy_period_public_id = ct.policy_period_public_id
    AND finca_0.coverage_pattern_code = ct.coverage_pattern_code
    AND finca_0.coverage_fixed_id = ct.coverage_fixed_id
    AND COALESCE(finca_0.vehicle_fixed_id, '') = COALESCE(ct.vehicle_fixed_id, '')
    AND COALESCE(finca_0.driver_fixed_id, '') = COALESCE(ct.driver_fixed_id, '')
    AND COALESCE(finca_0.dwelling_fixed_id, '') = COALESCE(ct.dwelling_fixed_id, '')
    AND finca_0.cost_subtype = ct.cost_subtype
    AND finca_0.sch_item_fixed_id = ct.sch_item_fixed_id
  WHERE
    (eff.public_id IS NULL
      OR
    IF
      (trns.uw_company_code = '08', GREATEST(DATE(finca_0.amount_effective_date), DATE(trns.edit_effective_date)), DATE(finca_0.amount_effective_date)) >= DATE(eff.effective_date)
      AND
    IF
      (trns.uw_company_code = '08', GREATEST(DATE(finca_0.amount_effective_date), DATE(trns.edit_effective_date)), DATE(finca_0.amount_effective_date)) < DATE(eff.expiration_date))
    AND (ct.policy_period_public_id IS NOT NULL
      OR trns.broker_code <> trns.prior_broker_code)
    AND (trns.uw_company_code = '01'
      OR DATE(finca_0.amount_effective_date) >= DATE(trns.edit_effective_date)
      OR DATE(finca_0.amount_expiration_date) > DATE(trns.edit_effective_date)
      AND ct.policy_period_public_id IS NOT NULL)
    AND DATE(finca_0.amount_effective_date) < DATE(trns.job_close_date)
    AND ABS(finca_0.net_term_amt) > 0
  UNION ALL
  SELECT
    finca_0.policy_account_number,
    finca_0.policy_number,
    finca_0.policy_period_start_date,
    finca_0.policy_period_end_date,
    trns.policy_period_public_id,
    trns.edit_effective_date,
    trns.job_close_date,
    trns.job_close_date AS accounting_date,
    finca_0.uw_company_code,
    finca_0.branch_number,
    COALESCE(eff.producer_code, trns.broker_code) AS producer_code_of_record_code,
    trns.job_subtype,
    finca_0.payment_plan_name,
    finca_0.product_code,
    finca_0.product_type,
    finca_0.product_desc,
    finca_0.coverable_province,
    finca_0.coverage_pattern_code,
    finca_0.net_term_amt AS net_term_amt,
    finca_0.risk_public_id,
    finca_0.model_number,
    finca_0.term_number,
    finca_0.cost_public_id,
    finca_0.cost_fixed_id,
    finca_0.orig_coverage_pattern_code,
    finca_0.external_lob_indicator,
    finca_0.agnt_inspector,
    finca_0.substandard_vehicle,
    finca_0.risk_type,
    finca_0.agnt_branch_number,
    finca_0.vehicle_type,
    'bckdt_earnings' AS job_source,
    billing_pp.billing_method,
    DATE_DIFF(LEAST(
      IF
        (trns.uw_company_code = '08', DATE(finca_0.amount_expiration_date), DATE(trns.model_date)), DATE(trns.model_date), DATE(trns.period_end)), GREATEST(DATE(finca_0.amount_effective_date), DATE(trns.edit_effective_date)), DAY) AS num_days
  FROM
    `@derived_project.product_gl.bckdtd_trans_staging` AS trns
  INNER JOIN
    `@derived_project.product_gl.finance_gl_financial_activity` AS finca_0
  ON
    finca_0.policy_period_public_id = trns.policy_period_public_id
    AND finca_0.financial_activity_category = 'COST'
    AND finca_0.removed_cov_new_old_broker_ind = ''
    AND finca_0.cost_subtype NOT IN( 'PersonalAutoShortRateCovCost_ECO',
      'PersonalVehicleShortRateCovCost_ECO',
      'HOShortRatePenaltyCost_ECO',
      'HomeownersShortRateCovCost_ECO',
      'DwellingShortRateCovCost_ECO',
      'ScheduleByTypeCovShortRateCost_ECO',
      'ScheduleCovShortRateCost_ECO' )
  INNER JOIN
    `@curated_project.gwpc.policy_period` AS billing_pp
  ON
    billing_pp.public_id = finca_0.policy_period_public_id
  LEFT OUTER JOIN
    `@curated_project.gwpc.eff_dated_fields` AS eff
  ON
    trns.policy_period_public_id = eff.policy_period_public_id
  LEFT OUTER JOIN (
    SELECT
      DISTINCT trns_0.policy_period_public_id,
      finca_1.coverage_pattern_code,
      finca_1.coverage_fixed_id,
      finca_1.vehicle_fixed_id,
      finca_1.driver_fixed_id,
      finca_1.dwelling_fixed_id,
      finca_1.cost_subtype,
      finca_1.sch_item_fixed_id
    FROM
      `@derived_project.product_gl.bckdtd_trans_staging` AS dly
    INNER JOIN
      `@curated_project.gwpc.transaction` AS trns_0
    ON
      dly.policy_period_public_id = trns_0.policy_period_public_id
    INNER JOIN
      `@derived_project.product_gl.finance_gl_financial_activity` AS finca_1
    ON
      finca_1.policy_period_public_id = trns_0.policy_period_public_id
      AND finca_1.financial_activity_category = 'TRANSACTION'
      AND finca_1.coverage_pattern_code <> 'ALL_COVERAGES' ) AS ct
  ON
    trns.policy_period_public_id = ct.policy_period_public_id
    AND finca_0.coverage_pattern_code = ct.coverage_pattern_code
    AND finca_0.coverage_fixed_id = ct.coverage_fixed_id
    AND COALESCE(finca_0.vehicle_fixed_id, '') = COALESCE(ct.vehicle_fixed_id, '')
    AND COALESCE(finca_0.driver_fixed_id, '') = COALESCE(ct.driver_fixed_id, '')
    AND COALESCE(finca_0.dwelling_fixed_id, '') = COALESCE(ct.dwelling_fixed_id, '')
    AND finca_0.cost_subtype = ct.cost_subtype
    AND finca_0.sch_item_fixed_id = ct.sch_item_fixed_id
  WHERE
    (eff.public_id IS NULL
      OR
    IF
      (trns.uw_company_code = '08', GREATEST(DATE(finca_0.amount_effective_date), DATE(trns.edit_effective_date)), DATE(finca_0.amount_effective_date)) >= DATE(eff.effective_date)
      AND
    IF
      (trns.uw_company_code = '08', GREATEST(DATE(finca_0.amount_effective_date), DATE(trns.edit_effective_date)), DATE(finca_0.amount_effective_date)) < DATE(eff.expiration_date))
    AND (ct.policy_period_public_id IS NOT NULL
      OR trns.broker_code <> trns.prior_broker_code)
    AND
  IF
    (trns.uw_company_code = '01'
      OR trns.job_subtype = 'Reinstatement'
      AND DATE(finca_0.amount_expiration_date) > DATE(trns.edit_effective_date), DATE(trns.edit_effective_date), DATE(finca_0.amount_effective_date)) >= DATE(trns.edit_effective_date)
    AND DATE(finca_0.amount_effective_date) < DATE(trns.job_close_date)
    AND ABS(finca_0.net_term_amt) > 0 ) AS finca
LEFT OUTER JOIN (
  SELECT
    plan_det.policy_period_public_id,
    plan_det.payment_plan_name,
    plan_det.edh_bc_sequence_id
  FROM (
    SELECT
      pp.public_id AS policy_period_public_id,
      bcpp.payment_plan_name,
      bcpp.edh_bc_sequence_id,
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
      DATE(bcpp.update_time) <= DATE(CAST(DATE(pp.model_date) + 1 AS DATETIME))
      OR bcpp.pc_policy_period_public_id = pp.public_id
      AND DATE(bcpp.update_time) <= DATE(pp.edit_effective_date) ) AS plan_det
  WHERE
    plan_det.plan_num = 1 ) AS pplan
ON
  pplan.policy_period_public_id = finca.policy_period_public_id
LEFT OUTER JOIN (
  SELECT
    plan_det.policy_period_public_id,
    plan_det.payment_plan_name AS payment_plan_name,
    plan_det.edh_bc_sequence_id
  FROM (
    SELECT
      pp.public_id AS policy_period_public_id,
      bcpp.payment_plan_name,
      bcpp.edh_bc_sequence_id,
      ROW_NUMBER() OVER (PARTITION BY pp.public_id ORDER BY bcpp.edh_bc_sequence_id) AS plan_num
    FROM
      `@curated_project.gwpc.pc_daily_transaction_staging` AS daily
    INNER JOIN
      `@curated_project.gwpc.policy_period` AS pp
    ON
      daily.public_id = pp.public_id
    LEFT OUTER JOIN
      `@curated_project.gwbc.policy_period` AS bcpp
    ON
      pp.policy_number = bcpp.policy_number
      AND CAST(pp.term_number AS STRING) = bcpp.term_number
    WHERE
      DATE(bcpp.update_time) > DATE(pp.model_date) ) AS plan_det
  WHERE
    plan_det.plan_num = 1 ) AS later_pplan
ON
  later_pplan.policy_period_public_id = finca.policy_period_public_id
LEFT OUTER JOIN (
  SELECT
    chg_0.prim_pol_comm_producer_code,
    chg_0.billing_method,
    chg_0.charge_commission_rate,
    chg_0.charge_pattern_code,
    chg_0.edh_bc_sequence_id,
    ROW_NUMBER() OVER (PARTITION BY chg_0.edh_bc_sequence_id, chg_0.charge_pattern_code ORDER BY chg_0.charge_date DESC) AS tran_order
  FROM
    `@curated_project.gwpc.pc_daily_transaction_staging` AS daily
  INNER JOIN
    `@curated_project.gwpc.policy_period` AS pp
  ON
    daily.public_id = pp.public_id
  LEFT OUTER JOIN
    `@curated_project.gwbc.policy_period` AS bcpp
  ON
    pp.policy_number = bcpp.policy_number
    AND CAST(pp.term_number AS STRING) = bcpp.term_number
  INNER JOIN
    `@curated_project.gwbc.charge` AS chg_0
  ON
    bcpp.edh_bc_sequence_id = chg_0.edh_bc_sequence_id ) AS chg
ON
  chg.tran_order = 1
  AND finca.risk_type = chg.charge_pattern_code
  AND COALESCE(pplan.edh_bc_sequence_id, later_pplan.edh_bc_sequence_id) = chg.edh_bc_sequence_id
LEFT OUTER JOIN
  `@derived_project.product_gl.finance_gl_class_of_business` AS gl_clob
ON
  gl_clob.coverage_pattern_code = finca.coverage_pattern_code
  AND
  CASE
    WHEN finca.product_code = 'PersonalAuto' AND gl_clob.rsp = 'N' THEN IF (finca.vehicle_type = '', 'auto', finca.vehicle_type)
  ELSE
  ''
END
  = gl_clob.vehicle_type
INNER JOIN
  `@derived_project.product_gl.finance_gl_accounting_rules` AS accounting_rule
ON
  accounting_rule.financial_activity_type =
  CASE
    WHEN finca.net_term_amt > 0 THEN 'ONSET'
  ELSE
  'OFFSET'
END
  AND accounting_rule.payment_plan_name = finca.payment_plan_name
  AND accounting_rule.class_of_business_code = gl_clob.class_of_business_code
  AND accounting_rule.amount_category = 'UNEARNED PREMIUM'
LEFT OUTER JOIN
  `@derived_project.product_gl.finance_gl_premium_tax_rate` AS premium_tax
ON
  premium_tax.province = finca.coverable_province
  AND premium_tax.class_of_business_code = gl_clob.class_of_business_code
LEFT OUTER JOIN
  `@derived_project.product_gl.finance_gl_dpae_rate` AS dpae
ON
  dpae.company_code = finca.uw_company_code
LEFT OUTER JOIN (
  SELECT
    agent_master.co_cd,
    agent_master.brk_num,
    agent_master.gw_brk_num,
    agent_master.branch_number,
    agent_master.insp_num,
    ROW_NUMBER() OVER (PARTITION BY agent_master.gw_brk_num ORDER BY agent_master.processed_date DESC) AS sel
  FROM
    `@curated_project.agnt.agent_master`  agent_master) AS agnt
ON
  agnt.sel = 1
  AND agnt.gw_brk_num = finca.producer_code_of_record_code
LEFT OUTER JOIN
  `@derived_project.product_gl.finance_gl_branch` AS branch
ON
  branch.company_code = agnt.co_cd
  AND branch.branch_number = agnt.branch_number
WHERE
  (accounting_rule.financial_activity_type IS NULL
    OR DATE(finca.job_close_date) BETWEEN DATE(accounting_rule.effective_start_date)
    AND DATE(accounting_rule.effective_end_date)
    AND (accounting_rule.amount_type IN( 'EARNED PREMIUMS',
        'EARNED TAXES' )
      OR accounting_rule.amount_type = 'EARNED DPAE'
      AND (DATE(finca.accounting_date) < CAST('2023-01-01' AS DATE)
        OR finca.uw_company_code <> '08')))
  AND (premium_tax.province IS NULL
    OR DATE(finca.policy_period_start_date) BETWEEN DATE(premium_tax.effective_start_date)
    AND DATE(premium_tax.effective_end_date))
  AND (dpae.company_code IS NULL
    OR DATE(finca.policy_period_start_date) BETWEEN DATE(dpae.effective_start_date)
    AND DATE(dpae.effective_end_date))
  AND (gl_clob.class_of_business_code IS NULL
    OR DATE(finca.job_close_date) BETWEEN DATE(gl_clob.effective_start_date)
    AND DATE(gl_clob.effective_end_date))
  AND ABS(finca.net_term_amt) > NUMERIC '0.0000' ;