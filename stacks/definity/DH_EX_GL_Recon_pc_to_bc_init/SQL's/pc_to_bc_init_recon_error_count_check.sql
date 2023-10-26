SELECT
    count(*) AS error_cnt
  FROM
    `@derived_project.product_gwbc_report.pc_to_bc_init_recon` AS pc_to_bc_init_recon
  WHERE pc_to_bc_init_recon.scheduler_yyyy_mm = '@scheduler_year_@scheduler_month'
   AND pc_to_bc_init_recon.job_id = substr('@lcv_execution_time', 1, 15)
   AND pc_to_bc_init_recon.edit_effective_date IS NOT NULL
;
