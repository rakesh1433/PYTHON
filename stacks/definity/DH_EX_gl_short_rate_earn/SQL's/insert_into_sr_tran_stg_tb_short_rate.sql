
INSERT INTO
  `@derived_project.product_gl.sr_trans_staging`
SELECT
  policy_account_number,
  policy_number,
  DATETIME(policy_period_start_date),
  DATETIME(policy_period_end_date),
  policy_period_public_id,
  DATETIME(edit_effective_date),
  DATETIME(job_close_date),
  DATETIME(accounting_date),
  uw_company_code,
  branch_number,
  producer_code_of_record_code,
  job_subtype,
  financial_activity_type,
  payment_plan_name,
  product_code,
  product_type,
  product_desc,
  coverable_province,
  coverage_pattern_code,
  class_of_business_code,
  amount_category,
  amount_sub_category,
  amount_type,
  accounting_entry_type,
  gl_account_number,
  actual_term_amount_amt,
  CAST(gl_amount_amt AS NUMERIC) AS actual_amount_amt,
  gl_amount_amt,
  tax_rate,
  dpae_rate,
  risk_public_id,
  model_number,
  term_number,
  cost_public_id,
  cost_fixed_id,
  orig_coverage_pattern_code,
  external_lob_indicator,
  agnt_inspector,
  substandard_vehicle,
  risk_type,
  brk_num,
  CAST(old_uw_company AS INT64),
  billing_method,
  commission_rate,
  CAST(agnt_branch_number AS STRING),
  vehicle_type
FROM (
  SELECT
    gl_financial_activity.policy_account_number,
    gl_financial_activity.policy_number,
    FORMAT_DATE("%FT%T", gl_financial_activity.policy_period_start_date) AS policy_period_start_date,
    FORMAT_DATE("%FT%T", gl_financial_activity.policy_period_end_date) AS policy_period_end_date,
    gl_financial_activity.policy_period_public_id,
    FORMAT_DATE("%FT%T", gl_financial_activity.policy_period_edit_effective_date) AS edit_effective_date,
    FORMAT_DATE("%FT%T", gl_financial_activity.job_close_date) AS job_close_date,
    FORMAT_DATE("%FT%T", gl_financial_activity.accounting_date) AS accounting_date,
    gl_financial_activity.uw_company_code,
    COALESCE(branch.gl_branch_number, COALESCE(CAST(agnt.branch_number AS STRING), gl_financial_activity.branch_number)) AS branch_number,
    gl_financial_activity.producer_code_of_record_code AS producer_code_of_record_code,
    gl_financial_activity.job_subtype,
    gl_financial_activity.financial_activity_type,
    CASE
      WHEN COALESCE(chg.billing_method, billing_pp.billing_method) = 'AgencyBill' THEN COALESCE(selected_plan.payment_plan_name, later_selected_plan.payment_plan_name, 'Agency Bill Annual Payment Plan')
    ELSE
    COALESCE(selected_plan.payment_plan_name, later_selected_plan.payment_plan_name, 'Monthly Payment Plan')
  END
    AS payment_plan_name,
    gl_financial_activity.product_code,
    gl_financial_activity.product_type,
    gl_financial_activity.coverable_province,
    gl_financial_activity.coverage_pattern_code,
    CASE gl_accounting_rules.amount_type
      WHEN 'EARNED PREMIUMS' THEN ROUND(COALESCE(ABS(gl_financial_activity.actual_term_amount_amt), NUMERIC '0.00'), 4)
      WHEN 'EARNED TAXES' THEN ROUND(COALESCE(ABS(gl_financial_activity.actual_term_amount_amt), NUMERIC '0.00'), 4) * (COALESCE(gl_premium_tax_rate.premium_tax_rate, NUMERIC '0.0000') + COALESCE(gl_premium_tax_rate.fire_tax_rate, NUMERIC '0.0000'))
      WHEN 'EARNED DPAE' THEN ROUND(COALESCE(ABS(gl_financial_activity.actual_term_amount_amt), NUMERIC '0.00'), 4) * COALESCE(dpae.dpae_rate, NUMERIC '0.0000')
    ELSE
    NUMERIC '0.00'
  END
    AS gl_amount_amt,
    gl_class_of_business.class_of_business_code,
    gl_accounting_rules.amount_category,
    gl_accounting_rules.accounting_entry_type,
    gl_accounting_rules.gl_account_number,
    gl_accounting_rules.amount_sub_category,
    gl_accounting_rules.amount_type,
    COALESCE(gl_financial_activity.actual_term_amount_amt, NUMERIC '0.00') AS actual_term_amount_amt,
    COALESCE(premium_tax_rate, NUMERIC '0.0000') + COALESCE(fire_tax_rate, NUMERIC '0.0000') AS tax_rate,
    NUMERIC '0.00' AS dpae_rate,
    gl_financial_activity.risk_public_id,
    gl_financial_activity.product_desc,
    DATE(gl_financial_activity.policy_period_edit_effective_date) AS edit_date,
    DATE(gl_financial_activity.job_close_date) AS close_date,
    gl_financial_activity.cost_public_id,
    gl_financial_activity.cost_fixed_id,
    gl_financial_activity.term_number,
    gl_financial_activity.model_number,
    gl_financial_activity.orig_coverage_pattern_code,
    'C' AS future_current_flag,
    gl_financial_activity.coverage_fixed_id,
    gl_financial_activity.external_lob_indicator,
    agnt.insp_num AS agnt_inspector,
    gl_financial_activity.substandard_vehicle,
    gl_financial_activity.risk_type,
    SUBSTR(COALESCE(agnt.gw_brk_num, chg.prim_pol_comm_producer_code), 3) AS brk_num,
    SUBSTR(COALESCE(agnt.gw_brk_num, chg.prim_pol_comm_producer_code), 1, 1) AS old_uw_company,
    COALESCE(chg.billing_method, billing_pp.billing_method) AS billing_method,
    chg.charge_commission_rate AS commission_rate,
    agnt.branch_number AS agnt_branch_number,
    gl_financial_activity.vehicle_type
  FROM
    `@curated_project.gwpc.pc_daily_transaction_staging` AS daily
  INNER JOIN
    `@derived_project.product_gl.finance_gl_financial_activity` AS gl_financial_activity
  ON
    daily.public_id = gl_financial_activity.policy_period_public_id
  INNER JOIN
    `@curated_project.gwpc.policy_period` AS billing_pp
  ON
    billing_pp.public_id = gl_financial_activity.policy_period_public_id
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
        `@curated_project.gwpc.pc_daily_transaction_staging` AS daily_0
      INNER JOIN
        `@curated_project.gwpc.policy_period` AS pp
      ON
        daily_0.public_id = pp.public_id
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
    gl_financial_activity.policy_period_public_id = selected_plan.policy_period_public_id
  LEFT OUTER JOIN (
    SELECT
      plan_det.policy_period_public_id,
      COALESCE(plan_det.payment_plan_name, 'Monthly Payment Plan') AS payment_plan_name,
      plan_det.edh_bc_sequence_id
    FROM (
      SELECT
        pp.public_id AS policy_period_public_id,
        bcpp.payment_plan_name,
        bcpp.edh_bc_sequence_id,
        ROW_NUMBER() OVER (PARTITION BY pp.public_id ORDER BY bcpp.edh_bc_sequence_id) AS plan_num
      FROM
        `@curated_project.gwpc.pc_daily_transaction_staging` AS daily_0
      INNER JOIN
        `@curated_project.gwpc.policy_period` AS pp
      ON
        daily_0.public_id = pp.public_id
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
    gl_financial_activity.policy_period_public_id = later_selected_plan.policy_period_public_id
  LEFT OUTER JOIN
    `@curated_project.gwpc.all_cost` AS pc_all_cost
  ON
    gl_financial_activity.cost_public_id = pc_all_cost.public_id
    AND gl_financial_activity.product_code = pc_all_cost.product
  LEFT OUTER JOIN (
    SELECT
      chg_0.prim_pol_comm_producer_code,
      chg_0.billing_method,
      chg_0.charge_commission_rate,
      chg_0.charge_pattern_code,
      chg_0.edh_bc_sequence_id,
      ROW_NUMBER() OVER (PARTITION BY chg_0.edh_bc_sequence_id, chg_0.charge_pattern_code ORDER BY chg_0.charge_date DESC) AS tran_order
    FROM
      `@curated_project.gwpc.pc_daily_transaction_staging` AS daily_0
    INNER JOIN
      `@curated_project.gwpc.policy_period` AS pp
    ON
      daily_0.public_id = pp.public_id
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
    AND pc_all_cost.risk_type = chg.charge_pattern_code
    AND COALESCE(selected_plan.edh_bc_sequence_id, later_selected_plan.edh_bc_sequence_id) = chg.edh_bc_sequence_id
  LEFT OUTER JOIN
    `@derived_project.product_gl.finance_gl_class_of_business` AS gl_class_of_business
  ON
    gl_class_of_business.coverage_pattern_code = gl_financial_activity.coverage_pattern_code
    AND
    CASE
      WHEN gl_financial_activity.product_code = 'PersonalAuto' AND gl_class_of_business.rsp = 'N' THEN IF (gl_financial_activity.vehicle_type = '', 'auto', gl_financial_activity.vehicle_type)
    ELSE
    ''
  END
    = gl_class_of_business.vehicle_type
  INNER JOIN
    `@derived_project.product_gl.finance_gl_accounting_rules` AS gl_accounting_rules
  ON
    gl_accounting_rules.financial_activity_type = gl_financial_activity.financial_activity_type
    AND gl_accounting_rules.payment_plan_name = COALESCE(selected_plan.payment_plan_name, COALESCE(later_selected_plan.payment_plan_name, 'Monthly Payment Plan'))
    AND gl_accounting_rules.class_of_business_code = gl_class_of_business.class_of_business_code
    AND gl_accounting_rules.amount_category = 'UNEARNED PREMIUM'
  LEFT OUTER JOIN
    `@derived_project.product_gl.finance_gl_premium_tax_rate` AS gl_premium_tax_rate
  ON
    gl_premium_tax_rate.province = gl_financial_activity.coverable_province
    AND gl_premium_tax_rate.class_of_business_code = gl_class_of_business.class_of_business_code
  LEFT OUTER JOIN
    `@derived_project.product_gl.finance_gl_dpae_rate` AS dpae
  ON
    dpae.company_code = gl_financial_activity.uw_company_code
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
    AND agnt.gw_brk_num = gl_financial_activity.producer_code_of_record_code
  LEFT OUTER JOIN
    `@derived_project.product_gl.finance_gl_branch` AS branch
  ON
    branch.company_code = agnt.co_cd
    AND branch.branch_number = agnt.branch_number
  WHERE
    pc_all_cost.subtype IN( 'PersonalAutoShortRateCovCost_ECO',
      'PersonalVehicleShortRateCovCost_ECO',
      'HOShortRatePenaltyCost_ECO',
      'HomeownersShortRateCovCost_ECO',
      'DwellingShortRateCovCost_ECO',
      'ScheduleByTypeCovShortRateCost_ECO',
      'ScheduleCovShortRateCost_ECO' )
    AND DATE(gl_financial_activity.policy_period_edit_effective_date) <= DATE(gl_financial_activity.job_close_date)
    AND (gl_accounting_rules.financial_activity_type IS NULL
      OR job_close_date BETWEEN DATE(gl_accounting_rules.effective_start_date)
      AND DATE(gl_accounting_rules.effective_end_date)
      AND (gl_accounting_rules.amount_type IN( 'EARNED PREMIUMS',
          'EARNED TAXES' )
        OR gl_accounting_rules.amount_type = 'EARNED DPAE'
        AND (DATE(gl_financial_activity.accounting_date) < CAST('2023-01-01' AS DATE)
          OR gl_financial_activity.uw_company_code <> '08')))
    AND gl_financial_activity.financial_activity_category = 'COST'
    AND (dpae.company_code IS NULL
      OR policy_period_start_date BETWEEN DATE(dpae.effective_start_date)
      AND DATE(dpae.effective_end_date))
    AND (gl_premium_tax_rate.province IS NULL
      OR policy_period_start_date BETWEEN DATE(gl_premium_tax_rate.effective_start_date)
      AND DATE(gl_premium_tax_rate.effective_end_date))
    AND (gl_class_of_business.coverage_pattern_code IS NULL
      OR job_close_date BETWEEN DATE(gl_class_of_business.effective_start_date)
      AND DATE(gl_class_of_business.effective_end_date))
  UNION ALL
  SELECT
    gl_financial_activity.policy_account_number,
    gl_financial_activity.policy_number,
    FORMAT_DATE("%FT%T", gl_financial_activity.policy_period_start_date) AS policy_period_start_date,
    FORMAT_DATE("%FT%T", gl_financial_activity.policy_period_end_date) AS policy_period_end_date,
    gl_financial_activity.policy_period_public_id,
    FORMAT_DATE("%FT%T", gl_financial_activity.policy_period_edit_effective_date) AS edit_effective_date,
    FORMAT_DATE("%FT%T", gl_financial_activity.job_close_date) AS job_close_date,
    FORMAT_DATE("%FT%T", (CAST(CONCAT(CAST(fd.acct_date AS STRING), ' 00:00:00') AS DATETIME))) AS accounting_date,
    gl_financial_activity.uw_company_code,
    COALESCE(branch.gl_branch_number, COALESCE(CAST(agnt.branch_number AS STRING), gl_financial_activity.branch_number)) AS branch_number,
    COALESCE(eff.producer_code, gl_financial_activity.producer_code_of_record_code) AS producer_code_of_record_code,
    gl_financial_activity.job_subtype,
    gl_financial_activity.financial_activity_type,
    CASE
      WHEN COALESCE(chg.billing_method, billing_pp.billing_method) = 'AgencyBill' THEN COALESCE(selected_plan.payment_plan_name, later_selected_plan.payment_plan_name, 'Agency Bill Annual Payment Plan')
    ELSE
    COALESCE(selected_plan.payment_plan_name, later_selected_plan.payment_plan_name, 'Monthly Payment Plan')
  END
    AS payment_plan_name,
    gl_financial_activity.product_code,
    gl_financial_activity.product_type,
    gl_financial_activity.coverable_province,
    gl_financial_activity.coverage_pattern_code,
    CASE gl_accounting_rules.amount_type
      WHEN 'EARNED PREMIUMS' THEN ROUND(COALESCE(ABS(gl_financial_activity.actual_term_amount_amt), NUMERIC '0.00'), 4)
      WHEN 'EARNED TAXES' THEN ROUND(COALESCE(ABS(gl_financial_activity.actual_term_amount_amt), NUMERIC '0.00'), 4) * (COALESCE(gl_premium_tax_rate.premium_tax_rate, NUMERIC '0.0000') + COALESCE(gl_premium_tax_rate.fire_tax_rate, NUMERIC '0.0000'))
      WHEN 'EARNED DPAE' THEN ROUND(COALESCE(ABS(gl_financial_activity.actual_term_amount_amt), NUMERIC '0.00'), 4) * COALESCE(dpae.dpae_rate, NUMERIC '0.0000')
    ELSE
    NUMERIC '0.00'
  END
    AS gl_amount_amt,
    gl_class_of_business.class_of_business_code,
    gl_accounting_rules.amount_category,
    gl_accounting_rules.accounting_entry_type,
    gl_accounting_rules.gl_account_number,
    gl_accounting_rules.amount_sub_category,
    gl_accounting_rules.amount_type,
    COALESCE(gl_financial_activity.actual_term_amount_amt, NUMERIC '0.00') AS actual_term_amount_amt,
    COALESCE(premium_tax_rate, NUMERIC '0.0000') + COALESCE(fire_tax_rate, NUMERIC '0.0000') AS tax_rate,
    NUMERIC '0.00' AS dpae_rate,
    gl_financial_activity.risk_public_id,
    gl_financial_activity.product_desc,
    DATE(gl_financial_activity.policy_period_edit_effective_date) AS edit_date,
    DATE(gl_financial_activity.job_close_date) AS close_date,
    gl_financial_activity.cost_public_id,
    gl_financial_activity.cost_fixed_id,
    gl_financial_activity.term_number,
    gl_financial_activity.model_number,
    gl_financial_activity.orig_coverage_pattern_code,
    CASE
      WHEN fd.acct_date > CAST('@execution_date' AS DATE) THEN 'F'
    ELSE
    'C'
  END
    AS future_current_flag,
    gl_financial_activity.coverage_fixed_id,
    gl_financial_activity.external_lob_indicator,
    agnt.insp_num AS agnt_inspector,
    gl_financial_activity.substandard_vehicle,
    gl_financial_activity.risk_type,
    SUBSTR(COALESCE(agnt.gw_brk_num, chg.prim_pol_comm_producer_code), 3) AS brk_num,
    SUBSTR(COALESCE(agnt.gw_brk_num, chg.prim_pol_comm_producer_code), 1, 1) AS old_uw_company,
    COALESCE(chg.billing_method, billing_pp.billing_method) AS billing_method,
    chg.charge_commission_rate AS commission_rate,
    agnt.branch_number AS agnt_branch_number,
    gl_financial_activity.vehicle_type
  FROM (
    SELECT
      DISTINCT future_dated2.policy_period_public_id,
      future_dated2.cost_public_id,
      future_dated2.coverage_pattern_code,
      future_dated2.orig_coverage_pattern_code,
      future_dated2.cost_fixed_id,
      COALESCE(acct_date_updated.acct_date, future_dated2.acct_date) AS acct_date
    FROM (
      SELECT
        fa.policy_period_public_id,
        DATE(fa.policy_period_edit_effective_date) AS acct_date,
        fa.policy_number,
        fa.term_number,
        fa.model_number,
        fa.cost_fixed_id,
        fa.cost_public_id,
        fa.coverage_pattern_code,
        fa.orig_coverage_pattern_code
      FROM
        `@curated_project.gwpc.pc_daily_transaction_staging` AS dly
      INNER JOIN
        `@derived_project.product_gl.finance_gl_financial_activity` AS fa
      ON
        dly.public_id = fa.policy_period_public_id
        AND fa.financial_activity_category = 'COST'
      WHERE
        DATE(fa.policy_period_edit_effective_date) > DATE(fa.job_close_date)
      UNION ALL
      SELECT
        DISTINCT pc_future_dated_transaction_staging.policy_period_public_id,
        DATE(pc_future_dated_transaction_staging.accounting_date) AS acct_date,
        CAST(pc_future_dated_transaction_staging.policy_number AS STRING),
        CAST(pc_future_dated_transaction_staging.term_number AS INT64),
        CAST(pc_future_dated_transaction_staging.model_number AS INT64),
        CAST(pc_future_dated_transaction_staging.cost_fixed_id AS STRING),
        CAST(pc_future_dated_transaction_staging.cost_public_id AS STRING),
        CAST(pc_future_dated_transaction_staging.coverage_pattern_code AS STRING),
        CAST(pc_future_dated_transaction_staging.orig_coverage_pattern_code AS STRING)
      FROM
        `@derived_project.product_gl.pc_future_dated_transaction_short_rate_staging` pc_future_dated_transaction_staging ) AS future_dated2
    LEFT OUTER JOIN (
      SELECT
        future_date_oos.policy_period_public_id,
        future_date_oos.cost_public_id,
        future_date_oos.coverage_pattern_code,
        future_date_oos.orig_coverage_pattern_code,
        future_date_oos.cost_fixed_id,
        COALESCE(future_date_oos.oos_acct_date, future_date_oos.fd_acct_date) AS acct_date
      FROM (
        SELECT
          future_dated.policy_period_public_id,
          future_dated.cost_public_id,
          future_dated.coverage_pattern_code,
          future_dated.orig_coverage_pattern_code,
          future_dated.cost_fixed_id,
          future_dated.acct_date AS fd_acct_date,
          oos.acct_date AS oos_acct_date,
          RANK() OVER (PARTITION BY future_dated.policy_period_public_id, future_dated.coverage_pattern_code, future_dated.orig_coverage_pattern_code, future_dated.cost_fixed_id ORDER BY oos.model_number DESC) AS rank
        FROM (
          SELECT
            fa.policy_period_public_id,
            DATE(fa.policy_period_edit_effective_date) AS acct_date,
            fa.policy_number,
            fa.term_number,
            fa.model_number,
            fa.cost_fixed_id,
            fa.cost_public_id,
            fa.coverage_pattern_code,
            fa.orig_coverage_pattern_code,
            fa.coverage_fixed_id
          FROM
            `@curated_project.gwpc.pc_daily_transaction_staging` AS dly
          INNER JOIN
            `@derived_project.product_gl.finance_gl_financial_activity` AS fa
          ON
            dly.public_id = fa.policy_period_public_id
            AND fa.financial_activity_category = 'COST'
          WHERE
            DATE(fa.policy_period_edit_effective_date) > DATE(fa.job_close_date)
          UNION ALL
          SELECT
            DISTINCT pc_future_dated_transaction_staging.policy_period_public_id,
            DATE(pc_future_dated_transaction_staging.accounting_date) AS acct_date,
            CAST(pc_future_dated_transaction_staging.policy_number AS STRING),
            CAST(pc_future_dated_transaction_staging.term_number AS INT64),
            CAST(pc_future_dated_transaction_staging.model_number AS INT64),
            CAST(pc_future_dated_transaction_staging.cost_fixed_id AS STRING),
            CAST(pc_future_dated_transaction_staging.cost_public_id AS STRING),
            CAST(pc_future_dated_transaction_staging.coverage_pattern_code AS STRING),
            CAST(pc_future_dated_transaction_staging.orig_coverage_pattern_code AS STRING),
            CAST(pc_future_dated_transaction_staging.coverage_fixed_id AS STRING)
          FROM
            `@derived_project.product_gl.pc_future_dated_transaction_short_rate_staging` pc_future_dated_transaction_staging ) AS future_dated
        INNER JOIN (
          SELECT
            GREATEST(DATE(pp.edit_effective_date), DATE(job.close_date)) AS acct_date,
            pp.policy_number,
            pp.term_number,
            pp.model_number,
            cost1.fixed_id AS cost_fixed_id,
            cost1.coverage_pattern_code,
            trn1.exp_date,
            cov.fixed_id AS coverage_fixed_id
          FROM
            `@curated_project.gwpc.pc_daily_transaction_staging` AS dly1
          INNER JOIN
            `@curated_project.gwpc.job` job
          ON
            dly1.public_id = job.policy_period_public_id
            AND job.oos_job
          INNER JOIN
            `@curated_project.gwpc.policy_period` AS pp
          ON
            dly1.public_id = pp.public_id
          INNER JOIN
            `@curated_project.gwpc.transaction` AS trn1
          ON
            dly1.public_id = trn1.policy_period_public_id
          INNER JOIN
            `@curated_project.gwpc.all_cost` AS cost1
          ON
            trn1.all_cost_public_id = cost1.public_id
            AND trn1.product = cost1.product
            AND cost1.charge_pattern = 'Premium'
          INNER JOIN
            `@curated_project.gwpc.all_coverage` AS cov
          ON
            cost1.coverage_public_id = cov.public_id
            AND cost1.coverage_pattern_code = cov.pattern_code
          GROUP BY
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8 ) AS oos
        ON
          future_dated.policy_number = oos.policy_number
          AND future_dated.term_number = oos.term_number
          AND future_dated.orig_coverage_pattern_code = oos.coverage_pattern_code
          AND future_dated.coverage_fixed_id = oos.coverage_fixed_id
        WHERE
          oos.model_number > future_dated.model_number
          AND future_dated.acct_date > oos.acct_date
          AND DATE(oos.exp_date) > future_dated.acct_date ) AS future_date_oos
      WHERE
        future_date_oos.rank = 1 ) AS acct_date_updated
    ON
      acct_date_updated.policy_period_public_id = future_dated2.policy_period_public_id
      AND acct_date_updated.cost_public_id = future_dated2.cost_public_id
      AND acct_date_updated.coverage_pattern_code = future_dated2.coverage_pattern_code
      AND acct_date_updated.orig_coverage_pattern_code = future_dated2.orig_coverage_pattern_code ) AS fd
  INNER JOIN
    `@derived_project.product_gl.finance_gl_financial_activity` AS gl_financial_activity
  ON
    fd.policy_period_public_id = gl_financial_activity.policy_period_public_id
    AND fd.cost_public_id = gl_financial_activity.cost_public_id
    AND fd.coverage_pattern_code = gl_financial_activity.coverage_pattern_code
    AND fd.orig_coverage_pattern_code = gl_financial_activity.orig_coverage_pattern_code
    AND gl_financial_activity.financial_activity_category = 'COST'
  INNER JOIN
    `@curated_project.gwpc.policy_period` billing_pp
  ON
    billing_pp.public_id = gl_financial_activity.policy_period_public_id
  LEFT OUTER JOIN (
    SELECT
      pp.public_id,
      recent_tran.public_id AS policy_period_public_id,
      ROW_NUMBER() OVER (PARTITION BY pp.public_id ORDER BY recent_tran.model_number DESC) AS rnk
    FROM (
      SELECT
        pc_daily_transaction_staging.public_id
      FROM
        `@curated_project.gwpc.pc_daily_transaction_staging` pc_daily_transaction_staging
      UNION ALL
      SELECT
        DISTINCT pc_future_dated_transaction_staging.policy_period_public_id AS public_id
      FROM
        `@derived_project.product_gl.pc_future_dated_transaction_short_rate_staging` pc_future_dated_transaction_staging ) AS dlytrn1
    INNER JOIN
      `@curated_project.gwpc.policy_period` AS pp
    ON
      dlytrn1.public_id = pp.public_id
    INNER JOIN
      `@curated_project.gwpc.policy_period` AS recent_tran
    ON
      pp.policy_number = recent_tran.policy_number
      AND pp.term_number = recent_tran.term_number
      AND recent_tran.model_number > pp.model_number
    WHERE
      DATE(GREATEST(recent_tran.edit_effective_date, recent_tran.model_date)) <= DATE(pp.edit_effective_date) ) AS recent_tran
  ON
    fd.policy_period_public_id = recent_tran.public_id
    AND recent_tran.rnk = 1
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
      FROM (
        SELECT
          pc_daily_transaction_staging.public_id
        FROM
          `@curated_project.gwpc.pc_daily_transaction_staging` pc_daily_transaction_staging
        UNION ALL
        SELECT
          DISTINCT pc_future_dated_transaction_staging.policy_period_public_id AS public_id
        FROM
          `@derived_project.product_gl.pc_future_dated_transaction_short_rate_staging` pc_future_dated_transaction_staging ) AS daily
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
    gl_financial_activity.policy_period_public_id = selected_plan.policy_period_public_id
  LEFT OUTER JOIN (
    SELECT
      plan_det.policy_period_public_id,
      COALESCE(plan_det.payment_plan_name, 'Monthly Payment Plan') AS payment_plan_name,
      plan_det.edh_bc_sequence_id
    FROM (
      SELECT
        pp.public_id AS policy_period_public_id,
        bcpp.payment_plan_name,
        bcpp.edh_bc_sequence_id,
        ROW_NUMBER() OVER (PARTITION BY pp.public_id ORDER BY bcpp.edh_bc_sequence_id) AS plan_num
      FROM (
        SELECT
          pc_daily_transaction_staging.public_id
        FROM
          `@curated_project.gwpc.pc_daily_transaction_staging` pc_daily_transaction_staging
        UNION ALL
        SELECT
          DISTINCT pc_future_dated_transaction_staging.policy_period_public_id AS public_id
        FROM
          `@derived_project.product_gl.pc_future_dated_transaction_short_rate_staging` pc_future_dated_transaction_staging ) AS daily
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
    gl_financial_activity.policy_period_public_id = later_selected_plan.policy_period_public_id
  LEFT OUTER JOIN
    `@derived_project.product_gl.finance_gl_class_of_business` AS gl_class_of_business
  ON
    gl_class_of_business.coverage_pattern_code = gl_financial_activity.coverage_pattern_code
    AND
    CASE
      WHEN gl_financial_activity.product_code = 'PersonalAuto' AND gl_class_of_business.rsp = 'N' THEN IF (gl_financial_activity.vehicle_type = '', 'auto', gl_financial_activity.vehicle_type)
    ELSE
    ''
  END
    = gl_class_of_business.vehicle_type
  LEFT OUTER JOIN
    `@curated_project.gwpc.all_cost` AS pc_all_cost
  ON
    gl_financial_activity.cost_public_id = pc_all_cost.public_id
    AND gl_financial_activity.product_code = pc_all_cost.product
  LEFT OUTER JOIN (
    SELECT
      chg_0.prim_pol_comm_producer_code,
      chg_0.billing_method,
      chg_0.charge_commission_rate,
      chg_0.charge_pattern_code,
      chg_0.edh_bc_sequence_id,
      ROW_NUMBER() OVER (PARTITION BY chg_0.edh_bc_sequence_id, chg_0.charge_pattern_code ORDER BY chg_0.charge_date DESC) AS tran_order
    FROM (
      SELECT
        pc_daily_transaction_staging.public_id
      FROM
        `@curated_project.gwpc.pc_daily_transaction_staging` pc_daily_transaction_staging
      UNION ALL
      SELECT
        DISTINCT pc_future_dated_transaction_staging.policy_period_public_id AS public_id
      FROM
        `@derived_project.product_gl.pc_future_dated_transaction_short_rate_staging` pc_future_dated_transaction_staging ) AS daily
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
    AND pc_all_cost.risk_type = chg.charge_pattern_code
    AND COALESCE(selected_plan.edh_bc_sequence_id, later_selected_plan.edh_bc_sequence_id) = chg.edh_bc_sequence_id
  INNER JOIN
    `@derived_project.product_gl.finance_gl_accounting_rules` AS gl_accounting_rules
  ON
    gl_accounting_rules.financial_activity_type = gl_financial_activity.financial_activity_type
    AND gl_accounting_rules.payment_plan_name = COALESCE(selected_plan.payment_plan_name, COALESCE(later_selected_plan.payment_plan_name, 'Monthly Payment Plan'))
    AND gl_accounting_rules.class_of_business_code = gl_class_of_business.class_of_business_code
    AND gl_accounting_rules.amount_category = 'UNEARNED PREMIUM'
  LEFT OUTER JOIN
    `@derived_project.product_gl.finance_gl_premium_tax_rate` AS gl_premium_tax_rate
  ON
    gl_premium_tax_rate.province = gl_financial_activity.coverable_province
    AND gl_premium_tax_rate.class_of_business_code = gl_class_of_business.class_of_business_code
  LEFT OUTER JOIN
    `@derived_project.product_gl.finance_gl_dpae_rate` AS dpae
  ON
    dpae.company_code = gl_financial_activity.uw_company_code
  LEFT OUTER JOIN
    `@curated_project.gwpc.eff_dated_fields` AS eff
  ON
    recent_tran.policy_period_public_id = eff.policy_period_public_id
  LEFT OUTER JOIN (
    SELECT
      agnt.co_cd,
      agnt.brk_num,
      agnt.gw_brk_num,
      agnt.branch_number,
      agnt.insp_num,
      ROW_NUMBER() OVER (PARTITION BY agnt.gw_brk_num ORDER BY agnt.processed_date DESC) AS sel
    FROM
      `@curated_project.agnt.agent_master` agnt ) AS agnt
  ON
    agnt.sel = 1
    AND agnt.gw_brk_num = COALESCE(eff.producer_code, gl_financial_activity.producer_code_of_record_code)
  LEFT OUTER JOIN
    `@derived_project.product_gl.finance_gl_branch` branch
  ON
    branch.company_code = agnt.co_cd
    AND branch.branch_number = agnt.branch_number
  WHERE
    pc_all_cost.subtype IN( 'PersonalAutoShortRateCovCost_ECO',
      'PersonalVehicleShortRateCovCost_ECO',
      'HOShortRatePenaltyCost_ECO',
      'HomeownersShortRateCovCost_ECO',
      'DwellingShortRateCovCost_ECO',
      'ScheduleByTypeCovShortRateCost_ECO',
      'ScheduleCovShortRateCost_ECO' )
    AND (gl_accounting_rules.financial_activity_type IS NULL
      OR DATE(job_close_date) BETWEEN DATE(gl_accounting_rules.effective_start_date)
      AND DATE(gl_accounting_rules.effective_end_date)
      AND (gl_accounting_rules.amount_type IN( 'EARNED PREMIUMS',
          'EARNED TAXES' )
        OR gl_accounting_rules.amount_type = 'EARNED DPAE'
        AND (fd.acct_date < CAST('2023-01-01' AS DATE)
          OR gl_financial_activity.uw_company_code <> '08')))
    AND (dpae.company_code IS NULL
      OR DATE(policy_period_start_date) BETWEEN DATE(dpae.effective_start_date)
      AND DATE(dpae.effective_end_date))
    AND (gl_premium_tax_rate.province IS NULL
      OR DATE(policy_period_start_date) BETWEEN DATE(gl_premium_tax_rate.effective_start_date)
      AND DATE(gl_premium_tax_rate.effective_end_date))
    AND (gl_class_of_business.coverage_pattern_code IS NULL
      OR DATE(job_close_date) BETWEEN DATE(gl_class_of_business.effective_start_date)
      AND DATE(gl_class_of_business.effective_end_date)) )
WHERE
  future_current_flag != 'F';