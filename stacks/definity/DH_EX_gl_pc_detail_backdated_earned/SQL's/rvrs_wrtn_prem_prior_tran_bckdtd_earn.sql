INSERT INTO
  `@derived_project.product_gl.finance_gl_pc_detail`
SELECT
  CAST('@execution_date' AS DATETIME) AS dlh_batch_ts,
  DATETIME(TIMESTAMP(FORMAT_DATETIME("%Y-%m-%dT%H:%M:%S",CURRENT_DATETIME("America/Toronto")))) AS dlh_process_ts,
  det.policy_account_number,
  det.policy_number,
  det.policy_period_start_date,
  det.policy_period_end_date,
  det.policy_period_public_id,
  det.edit_effective_date,
  det.job_close_date,
  det.accounting_date,
  det.uw_company_code,
  det.branch_number,
  det.producer_code_of_record_code,
  det.job_subtype,
  det.financial_activity_type,
  det.payment_plan_name,
  det.product_code,
  det.product_type,
  det.product_desc,
  det.coverable_province,
  det.coverage_pattern_code,
  det.class_of_business_code,
  det.amount_category,
  det.amount_sub_category,
  det.amount_type,
  det.accounting_entry_type,
  det.gl_account_number,
  CAST(ROUND(det.actual_term_amount_amt, 4) AS NUMERIC) AS actual_term_amount_amt,
  CAST(ROUND(det.actual_amount_amt, 4) AS NUMERIC) AS actual_amount_amt,
  CAST(ROUND(ABS(det.actual_amount_amt), 4) AS NUMERIC) AS gl_amount_amt,
  CAST(ROUND(det.tax_rate, 4) AS NUMERIC) AS tax_rate,
  CAST(ROUND(det.dpae_rate, 4) AS NUMERIC) AS dpae_rate,
  det.risk_public_id,
  det.model_number,
  det.term_number,
  det.cost_public_id,
  det.cost_fixed_id,
  det.coverage_pattern_code AS orig_coverage_pattern_code,
  det.external_lob_indicator,
  det.agnt_inspector,
  det.substandard_vehicle AS substandard_vehicle,
  det.risk_type,
  det.brk_num,
  CAST(det.old_uw_company AS INT64) AS old_uw_company,
  det.billing_method,
  det.commission_rate AS commission_rate,
  det.agnt_branch_number,
  det.vehicle_type,
  'rev_wp_bckdt_earn' AS job_source,
  CAST(FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CAST(TIMESTAMP_SECONDS(UNIX_SECONDS(CURRENT_TIMESTAMP())) AS DATETIME)) AS DATETIME) AS processed_date,
  EXTRACT(YEAR
  FROM
    CAST(det.accounting_date AS DATETIME)) AS acc_year,
  EXTRACT(MONTH
  FROM
    CAST(det.accounting_date AS DATETIME)) AS acc_month,
  EXTRACT(DAY
  FROM
    CAST(det.accounting_date AS DATETIME)) AS acc_day
FROM (
  SELECT
    finca.policy_account_number,
    finca.policy_number,
    finca.policy_period_start_date AS policy_period_start_date,
    finca.policy_period_end_date AS policy_period_end_date,
    pol.public_id AS policy_period_public_id,
    pol.edit_effective_date,
    pol.job_close_date AS job_close_date,
    pol.job_close_date AS accounting_date,
    finca.uw_company_code,
    CASE finca.uw_company_code
      WHEN '08' THEN finca.branch_number
    ELSE
    COALESCE(branch.gl_branch_number, COALESCE(CAST(agnt.branch_number AS STRING), finca.branch_number))
  END
    AS branch_number,
  IF
    (bto_rows.removed_cov_new_old_broker_ind = 'n', pol.cur_producer_code, pol.ppv_producer_code) AS producer_code_of_record_code,
    finca.job_subtype,
  IF
    (CAST(
      IF
        (bto_rows.removed_cov_new_old_broker_ind = 'n', finca.actual_amount_amt, finca.actual_amount_amt * -1) AS BIGNUMERIC) > NUMERIC '0.00', 'ONSET', 'OFFSET') AS financial_activity_type,
    CASE
      WHEN COALESCE(chg.billing_method, billing_pp.billing_method) = 'AgencyBill' THEN COALESCE(selected_plan.payment_plan_name, later_selected_plan.payment_plan_name, 'Agency Bill Annual Payment Plan')
    ELSE
    COALESCE(selected_plan.payment_plan_name, later_selected_plan.payment_plan_name, 'Monthly Payment Plan')
  END
    AS payment_plan_name,
    finca.product_code,
    finca.product_type,
    finca.coverable_province,
    finca.coverage_pattern_code,
    CASE gl_accounting_rules.amount_type
      WHEN 'PREMIUM TAXES' THEN ROUND(CAST( IF (bto_rows.removed_cov_new_old_broker_ind = 'n', finca.actual_amount_amt, finca.actual_amount_amt * -1) AS BIGNUMERIC) * (COALESCE(gl_premium_tax_rate.premium_tax_rate, NUMERIC '0.0000') + COALESCE(gl_premium_tax_rate.fire_tax_rate, NUMERIC '0.0000')), 4)
      WHEN 'DEFERRED TAXES' THEN ROUND(CAST(
      IF
        (bto_rows.removed_cov_new_old_broker_ind = 'n', finca.actual_amount_amt, finca.actual_amount_amt * -1) AS BIGNUMERIC) * (COALESCE(gl_premium_tax_rate.premium_tax_rate, NUMERIC '0.0000') + COALESCE(gl_premium_tax_rate.fire_tax_rate, NUMERIC '0.0000')), 4)
      WHEN 'DEFERRED DPAE' THEN ROUND(CAST( IF (bto_rows.removed_cov_new_old_broker_ind = 'n', finca.actual_amount_amt, finca.actual_amount_amt * -1) AS BIGNUMERIC) * COALESCE(gl_dpae_rate.dpae_rate, NUMERIC '0.0000'), 4)
    ELSE
    ROUND(CAST(
      IF
        (bto_rows.removed_cov_new_old_broker_ind = 'n', finca.actual_amount_amt, finca.actual_amount_amt * -1) AS BIGNUMERIC), 4)
  END
    AS actual_amount_amt,
    gl_accounting_rules.class_of_business_code,
    gl_accounting_rules.amount_category,
    gl_accounting_rules.accounting_entry_type,
    gl_accounting_rules.gl_account_number,
    gl_accounting_rules.amount_sub_category,
    gl_accounting_rules.amount_type,
    COALESCE(finca.actual_term_amount_amt, NUMERIC '0.00') AS actual_term_amount_amt,
  IF
    (gl_accounting_rules.amount_type IN( 'PREMIUM TAXES',
        'DEFERRED TAXES' ), COALESCE(premium_tax_rate, NUMERIC '0.0000') + COALESCE(fire_tax_rate, NUMERIC '0.0000'), NUMERIC '0.0000') AS tax_rate,
  IF
    (gl_accounting_rules.amount_type IN( 'DEFERRED DPAE' ), COALESCE(gl_dpae_rate.dpae_rate, NUMERIC '0.0000'), NUMERIC '0.0000') AS dpae_rate,
    finca.risk_public_id,
    finca.product_desc,
    DATE(finca.policy_period_edit_effective_date) AS edit_date,
    DATE(finca.job_close_date) AS close_date,
    finca.cost_public_id,
    finca.cost_fixed_id,
    finca.term_number,
    finca.model_number,
    finca.orig_coverage_pattern_code,
    'C' AS future_current_flag,
    finca.coverage_fixed_id,
    finca.external_lob_indicator,
    agnt.insp_num AS agnt_inspector,
    finca.substandard_vehicle,
    finca.risk_type,
    SUBSTR(COALESCE(agnt.gw_brk_num, chg.prim_pol_comm_producer_code), 3) AS brk_num,
    SUBSTR(COALESCE(agnt.gw_brk_num, chg.prim_pol_comm_producer_code), 1, 1) AS old_uw_company,
    COALESCE(chg.billing_method, billing_pp.billing_method) AS billing_method,
    chg.charge_commission_rate AS commission_rate,
    CAST(agnt.branch_number AS STRING) AS agnt_branch_number,
    finca.vehicle_type
  FROM (
    SELECT
      pp.public_id,
      pp.job_close_date,
      ppv.public_id AS ppv_public_id,
      ppv1.public_id AS ppv1_public_id,
      pp.edit_effective_date,
      ppv.edit_effective_date AS prior_edit_effective_date,
      pp.broker_code AS cur_producer_code,
      ppv_tran.reported_producer_code AS ppv_producer_code,
      ROW_NUMBER() OVER (PARTITION BY pp.public_id, ppv.public_id ORDER BY COALESCE(future.model_number, ppv.model_number) DESC) AS sel
    FROM
      `@derived_project.product_gl.bckdtd_trans_staging` AS pp
    INNER JOIN
      `@curated_project.gwpc.policy_period` AS ppv
    ON
      pp.policy_number = ppv.policy_number
      AND pp.term_number = ppv.term_number
      AND pp.model_number > ppv.model_number
    INNER JOIN
      `@curated_project.gwpc.policy_period` AS ppv1
    ON
      pp.policy_number = ppv1.policy_number
      AND pp.term_number = ppv1.term_number
      AND pp.model_number - 1 = ppv1.model_number
    LEFT OUTER JOIN
      `@curated_project.gwpc.policy_period` AS future
    ON
      pp.policy_number = future.policy_number
      AND pp.term_number = future.term_number
      AND future.model_number > ppv.model_number
      AND future.model_number < pp.model_number
      AND DATE(ppv.edit_effective_date) >= DATE(future.edit_effective_date)
    LEFT OUTER JOIN
      `@derived_project.product_gl.effective_trans_staging` AS ppv_tran
    ON
      COALESCE(future.public_id, ppv.public_id) = ppv_tran.policy_period_public_id
      AND ppv_tran.rnk = 1
    WHERE
      DATE(ppv.edit_effective_date) >= DATE(pp.edit_effective_date)
      AND DATE(ppv.edit_effective_date) < DATE(pp.job_close_date) ) AS pol
  INNER JOIN
    `@derived_project.product_gl.finance_gl_financial_activity` AS finca
  ON
    pol.ppv_public_id = finca.policy_period_public_id
    AND pol.cur_producer_code <> pol.ppv_producer_code
    AND finca.financial_activity_category = 'TRANSACTION'
    AND pol.sel = 1
  INNER JOIN
    `@curated_project.gwpc.policy_period` AS billing_pp
  ON
    billing_pp.public_id = finca.policy_period_public_id
  CROSS JOIN (
    SELECT
      'n' AS removed_cov_new_old_broker_ind
    UNION ALL
    SELECT
      'o' AS removed_cov_new_old_broker_ind ) AS bto_rows
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
        `@curated_project.gwpc.pc_daily_transaction_staging` AS daily
      INNER JOIN
        `@curated_project.gwpc.policy_period` AS pp
      ON
        daily.public_id = pp.public_id
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
      plan_det.plan_num = 1 ) AS selected_plan
  ON
    finca.policy_period_public_id = selected_plan.policy_period_public_id
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
      plan_det.plan_num = 1 ) AS later_selected_plan
  ON
    finca.policy_period_public_id = later_selected_plan.policy_period_public_id
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
    AND COALESCE(selected_plan.edh_bc_sequence_id, later_selected_plan.edh_bc_sequence_id) = chg.edh_bc_sequence_id
  LEFT OUTER JOIN
    `@derived_project.product_gl.finance_gl_class_of_business` AS gl_class_of_business
  ON
    gl_class_of_business.coverage_pattern_code = finca.coverage_pattern_code
    AND
    CASE
      WHEN finca.product_code = 'PersonalAuto' AND finca.coverage_pattern_code <> 'ALL_COVERAGES' AND gl_class_of_business.rsp = 'N' THEN IF (finca.vehicle_type = '', 'auto', finca.vehicle_type)
    ELSE
    ''
  END
    = gl_class_of_business.vehicle_type
  INNER JOIN
    `@derived_project.product_gl.finance_gl_accounting_rules` AS gl_accounting_rules
  ON
    gl_accounting_rules.financial_activity_type =
  IF
    (CAST(
      IF
        (bto_rows.removed_cov_new_old_broker_ind = 'n', finca.actual_amount_amt, finca.actual_amount_amt * -1) AS BIGNUMERIC) > NUMERIC '0.00', 'ONSET', 'OFFSET')
    AND gl_accounting_rules.payment_plan_name = COALESCE(selected_plan.payment_plan_name, COALESCE(later_selected_plan.payment_plan_name, 'Monthly Payment Plan'))
    AND gl_accounting_rules.class_of_business_code = gl_class_of_business.class_of_business_code
    AND gl_accounting_rules.amount_category IN( 'PREMIUM',
      'UNEARNED PREMIUM' )
  LEFT OUTER JOIN
    `@derived_project.product_gl.finance_gl_class_of_business` AS prem_tax_clob
  ON
    prem_tax_clob.coverage_pattern_code =
    CASE
      WHEN finca.coverage_pattern_code <> 'ALL_COVERAGES' THEN finca.coverage_pattern_code
    ELSE
    finca.orig_coverage_pattern_code
  END
    AND
    CASE
      WHEN finca.product_code = 'PersonalAuto' AND prem_tax_clob.rsp = 'N' THEN IF (finca.vehicle_type = '', 'auto', finca.vehicle_type)
    ELSE
    ''
  END
    = prem_tax_clob.vehicle_type
  LEFT OUTER JOIN
    `@derived_project.product_gl.finance_gl_premium_tax_rate` AS gl_premium_tax_rate
  ON
    gl_premium_tax_rate.province = finca.coverable_province
    AND gl_premium_tax_rate.class_of_business_code = prem_tax_clob.class_of_business_code
  LEFT OUTER JOIN
    `@derived_project.product_gl.finance_gl_dpae_rate` AS gl_dpae_rate
  ON
    gl_dpae_rate.company_code = finca.uw_company_code
  LEFT OUTER JOIN (
    SELECT
      agent_master.co_cd,
      agent_master.brk_num,
      agent_master.gw_brk_num,
      agent_master.branch_number,
      agent_master.insp_num,
      ROW_NUMBER() OVER (PARTITION BY agent_master.gw_brk_num ORDER BY agent_master.processed_date DESC) AS sel
    FROM
      `@curated_project.agnt.agent_master` agent_master ) AS agnt
  ON
    agnt.sel = 1
    AND agnt.gw_brk_num =
  IF
    (bto_rows.removed_cov_new_old_broker_ind = 'n', pol.cur_producer_code, pol.ppv_producer_code)
  LEFT OUTER JOIN
    `@derived_project.product_gl.finance_gl_branch` AS branch
  ON
    branch.company_code = agnt.co_cd
    AND branch.branch_number = agnt.branch_number
  WHERE
    (gl_accounting_rules.amount_category <> 'UNEARNED PREMIUM'
      OR finca.coverage_pattern_code <> 'ALL_COVERAGES')
    AND (gl_accounting_rules.financial_activity_type IS NULL
      OR DATE(pol.job_close_date) BETWEEN DATE(gl_accounting_rules.effective_start_date)
      AND DATE(gl_accounting_rules.effective_end_date)
      AND (gl_accounting_rules.amount_type IN( 'WRITTEN PREMIUMS',
          'PREMIUM TAXES',
          'UNEARNED PREMIUMS',
          'DEFERRED TAXES' )
        OR gl_accounting_rules.amount_type = 'DEFERRED DPAE'
        AND (DATE(pol.job_close_date) < CAST('2023-01-01' AS DATE)
          OR finca.uw_company_code <> '08')))
    AND (gl_premium_tax_rate.province IS NULL
      OR DATE(policy_period_start_date) BETWEEN DATE(gl_premium_tax_rate.effective_start_date)
      AND DATE(gl_premium_tax_rate.effective_end_date))
    AND (gl_dpae_rate.company_code IS NULL
      OR DATE(policy_period_start_date) BETWEEN DATE(gl_dpae_rate.effective_start_date)
      AND DATE(gl_dpae_rate.effective_end_date))
    AND (gl_class_of_business.coverage_pattern_code IS NULL
      OR DATE(pol.job_close_date) BETWEEN DATE(gl_class_of_business.effective_start_date)
      AND DATE(gl_class_of_business.effective_end_date))
    AND (prem_tax_clob.coverage_pattern_code IS NULL
      OR DATE(pol.job_close_date) BETWEEN DATE(prem_tax_clob.effective_start_date)
      AND DATE(prem_tax_clob.effective_end_date)) ) AS det
WHERE
  ABS(det.actual_amount_amt) > NUMERIC '0.0000' ;