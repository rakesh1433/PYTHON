INSERT INTO `@derived_project.product_gwbc_report.pc_to_bc_init_recon`
  SELECT
  CAST('@schedule_date' AS DATETIME) as dlh_batch_ts,
      CAST(FORMAT_DATETIME("%Y-%m-%dT%H:%M:%S", (CURRENT_DATETIME("America/Toronto"))) AS DATETIME) as dlh_process_ts,
      pc_to_bc_init_recon.policy_number AS policy_number,
      pc_to_bc_init_recon.policy_period_public_id AS policy_period_public_id,
      pc_to_bc_init_recon.edit_effective_date AS edit_effective_date,
      pc_to_bc_init_recon.uw_company_code AS uw_company_code,
      pc_to_bc_init_recon.producer_code_of_record AS producer_code_of_record,
      pc_to_bc_init_recon.payment_plan_name AS payment_plan_name,
      pc_to_bc_init_recon.product_code AS product_code,
      pc_to_bc_init_recon.all_cost_public_id AS all_cost_public_id,
      pc_to_bc_init_recon.covergae_pattern_code AS covergae_pattern_code,
      pc_to_bc_init_recon.risk_type AS risk_type,
      pc_to_bc_init_recon.vehice_type AS vehice_type,
      pc_to_bc_init_recon.amount_amt AS amount_amt,
      pc_to_bc_init_recon.reason_code AS reason_code,
      pc_to_bc_init_recon.ingested_date AS ingested_date,
      pc_to_bc_init_recon.job_run_date AS job_run_date,
      pc_to_bc_init_recon.job_id AS job_id,
      pc_to_bc_init_recon.scheduler_timestamp AS scheduler_timestamp,
      pc_to_bc_init_recon.scheduler_date AS scheduler_date,
      pc_to_bc_init_recon.scheduler_yyyy_mm AS scheduler_yyyy_mm
    FROM
      `@curated_project.gwbc.pc_to_bc_init_recon` AS pc_to_bc_init_recon
    WHERE substr('@lcv_execution_time', 1, 15) = pc_to_bc_init_recon.job_id
  UNION ALL
  SELECT
      det.*,
    FROM
      (
        SELECT
        CAST('@schedule_date' AS DATETIME) as dlh_batch_ts,
      CAST(FORMAT_DATETIME("%Y-%m-%dT%H:%M:%S", (CURRENT_DATETIME("America/Toronto"))) AS DATETIME) as dlh_process_ts,
            '' AS policy_number,
            '' AS policy_period_public_id,
            CAST(NULL as DATETIME) AS edit_effective_date,
            '' AS uw_company_code,
            '' AS producer_code_of_record,
            '' AS payment_plan_name,
            '' AS product_code,
            '' AS all_cost_public_id,
            '' AS covergae_pattern_code,
            '' AS risk_type,
            '' AS vehice_type,
            CAST(NULL as NUMERIC) AS amount_amt,
            '' AS reason_code,
            '' AS ingested_date,
            CAST(format_timestamp('%Y-%m-%d %H:%M:%S', timestamp_seconds(unix_seconds(safe.parse_timestamp('%Y%m%d%H%M%S', '@lcv_execution_time')))) AS DATETIME) AS job_run_date,
            substr('@lcv_execution_time', 1, 15) AS job_id,
            CAST(format_datetime('%Y-%m-%d %H:%M:%S', CAST(timestamp_seconds(unix_seconds(safe.parse_timestamp('%Y-%m-%d', '@schedule_date'))) as DATETIME)) AS DATETIME) AS job_scheduler_timestamp,
            '@schedule_date' AS job_scheduler_date,
            '@scheduler_year_@scheduler_month' AS scheduler_yyyy_mm
      ) AS det
 where (select count(*) from `@curated_project.gwbc.pc_to_bc_init_recon` rc where substr('@lcv_execution_time',1,15) =  rc.job_id) = 0
 ;
