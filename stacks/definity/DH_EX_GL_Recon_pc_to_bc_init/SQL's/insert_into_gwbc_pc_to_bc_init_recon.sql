INSERT INTO `@curated_project.gwbc.pc_to_bc_init_recon`
  SELECT
  CAST('@schedule_date' AS DATETIME) as dlh_batch_ts,
      CAST(FORMAT_DATETIME("%Y-%m-%dT%H:%M:%S", (CURRENT_DATETIME("America/Toronto"))) AS DATETIME) as dlh_process_ts,
      pp.policy_number,
      pp.public_id AS policy_period_public_id,
      pp.edit_effective_date,
      pp.uw_company_code,
      pp.prod_cos_code AS producer_code_of_record_code,
      coalesce(selected_plan.payment_plan_name, later_selected_plan.payment_plan_name) AS payment_plan_name,
      policy.product_code,
      pc_transaction.all_cost_public_id,
      pc_all_cost.coverage_pattern_code,
      pc_all_cost.risk_type,
      vehicle.vehicle_type,
      pc_transaction.amount_amt,
      CASE
        WHEN selected_plan.payment_plan_name IS NULL
         AND later_selected_plan.payment_plan_name IS NULL THEN 'No init record'
        WHEN gl_class_of_business.class_of_business_code IS NULL THEN 'No class of business found for coverage pattern code and/or vehicle risk type'
        WHEN gl_accounting_rules.gl_account_number IS NULL THEN 'No accounting rules found for payment plan and class of business'
        WHEN pp.uw_company_code = '01'
         AND chg.charge_commission_rate IS NULL THEN 'null comm rate or no charge record found for risk type'
        ELSE ''
      END AS reason_code,
      CAST(DATE(pp.processed_date) AS STRING) AS ingested_date,
      CAST(format_timestamp('%Y-%m-%d %H:%M:%S', timestamp_seconds(unix_seconds(safe.parse_timestamp('%Y%m%d%H%M%S', '@lcv_execution_time')))) AS DATETIME) AS job_run_date,
      substr('@lcv_execution_time', 1, 15) AS job_id,
      CAST(format_datetime('%Y-%m-%d %H:%M:%S', CAST(timestamp_seconds(unix_seconds(safe.parse_timestamp('%Y-%m-%d', '@schedule_date'))) as DATETIME)) AS DATETIME) AS job_scheduler_timestamp,
      '@schedule_date' AS job_scheduler_date,
      '@scheduler_year_@scheduler_month' AS scheduler_yyyy_mm
    FROM
      `@curated_project.gwpc.transaction` AS pc_transaction
      INNER JOIN `@curated_project.gwpc.all_cost` AS pc_all_cost ON pc_all_cost.public_id = pc_transaction.all_cost_public_id
       AND pc_all_cost.product = pc_transaction.product
       AND pc_all_cost.charge_pattern = 'Premium'
      INNER JOIN `@curated_project.gwpc.policy_period` AS pp ON pc_all_cost.policy_period_public_id = pp.public_id
      INNER JOIN `@curated_project.gwpc.policy` AS policy ON pp.public_id = policy.policy_period_public_id
      LEFT OUTER JOIN (
        SELECT
            plan_det.policy_period_public_id,
            plan_det.payment_plan_name,
            plan_det.edh_bc_sequence_id
          FROM
            (
              SELECT
                  pp_0.public_id AS policy_period_public_id,
                  bcpp.payment_plan_name,
                  bcpp.edh_bc_sequence_id,
                  rank() OVER (PARTITION BY pp_0.public_id ORDER BY bcpp.edh_bc_sequence_id DESC) AS plan_num
                FROM
                  `@curated_project.gwpc.policy_period` AS pp_0
                  INNER JOIN `@curated_project.gwbc.policy_period` AS bcpp ON pp_0.policy_number = bcpp.policy_number
                   AND CAST(pp_0.term_number AS STRING) = bcpp.term_number
                WHERE DATE(bcpp.update_time) <= DATE(CAST(DATE(pp_0.model_date) + 1 as DATETIME))
                 OR bcpp.pc_policy_period_public_id = pp_0.public_id
                 AND DATE(bcpp.update_time) <= DATE(pp_0.edit_effective_date)
            ) AS plan_det
          WHERE plan_det.plan_num = 1
      ) AS selected_plan ON pp.public_id = selected_plan.policy_period_public_id
      LEFT OUTER JOIN (
        SELECT
            plan_det.policy_period_public_id,
            plan_det.payment_plan_name AS payment_plan_name,
            plan_det.edh_bc_sequence_id
          FROM
            (
              SELECT
                  pp_0.public_id AS policy_period_public_id,
                  bcpp.payment_plan_name,
                  bcpp.edh_bc_sequence_id,
                  rank() OVER (PARTITION BY pp_0.public_id ORDER BY bcpp.edh_bc_sequence_id) AS plan_num
                FROM
                  `@curated_project.gwpc.policy_period` AS pp_0
                  INNER JOIN `@curated_project.gwbc.policy_period` AS bcpp ON pp_0.policy_number = bcpp.policy_number
                   AND CAST(pp_0.term_number AS STRING) = bcpp.term_number
                WHERE DATE(bcpp.update_time) > DATE(pp_0.model_date)
            ) AS plan_det
          WHERE plan_det.plan_num = 1
      ) AS later_selected_plan ON pp.public_id = later_selected_plan.policy_period_public_id
      LEFT OUTER JOIN `@curated_project.gwpc.vehicle` AS vehicle ON pc_all_cost.vehicle_public_id = vehicle.public_id
       AND pc_all_cost.policy_period_public_id = vehicle.policy_period_public_id
      LEFT OUTER JOIN `@derived_project.product_gl.finance_gl_class_of_business` AS gl_class_of_business ON gl_class_of_business.coverage_pattern_code = pc_all_cost.coverage_pattern_code
       AND gl_class_of_business.vehicle_type = CASE
         policy.product_code
        WHEN 'Homeowners' THEN ''
        ELSE if(vehicle.vehicle_type = '', 'auto', vehicle.vehicle_type)
      END
       AND gl_class_of_business.rsp = CASE
         policy.product_code
        WHEN 'PersonalAuto' THEN 'N'
        ELSE ''
      END
      LEFT OUTER JOIN `@derived_project.product_gl.finance_gl_accounting_rules` AS gl_accounting_rules ON gl_accounting_rules.financial_activity_type = 'ONSET'
       AND gl_accounting_rules.payment_plan_name = coalesce(selected_plan.payment_plan_name, later_selected_plan.payment_plan_name)
       AND gl_accounting_rules.class_of_business_code = gl_class_of_business.class_of_business_code
       AND gl_accounting_rules.amount_category = 'PREMIUM'
       AND gl_accounting_rules.amount_type = 'WRITTEN PREMIUMS'
      LEFT OUTER JOIN (
        SELECT
            chg_0.prim_pol_comm_producer_code,
            chg_0.billing_method,
            chg_0.charge_commission_rate,
            chg_0.charge_pattern_code,
            chg_0.edh_bc_sequence_id,
            row_number() OVER (PARTITION BY chg_0.edh_bc_sequence_id, chg_0.charge_pattern_code ORDER BY chg_0.charge_date DESC) AS tran_order
          FROM
            `@curated_project.gwbc.charge` AS chg_0
      ) AS chg ON chg.tran_order = 1
       AND pc_all_cost.risk_type = chg.charge_pattern_code
       AND coalesce(selected_plan.edh_bc_sequence_id, later_selected_plan.edh_bc_sequence_id) = chg.edh_bc_sequence_id
    WHERE DATE(pc_transaction.processed_date) BETWEEN CAST('@processed_startdate' as DATE) AND CAST('@processed_enddate' as DATE)
     AND abs(pc_transaction.amount_amt) > NUMERIC '0.0000'
     AND (selected_plan.payment_plan_name IS NULL
     AND later_selected_plan.payment_plan_name IS NULL
     OR gl_class_of_business.class_of_business_code IS NULL
     OR gl_accounting_rules.gl_account_number IS NULL
     OR pp.uw_company_code = '01'
     AND chg.charge_commission_rate IS NULL)
;
