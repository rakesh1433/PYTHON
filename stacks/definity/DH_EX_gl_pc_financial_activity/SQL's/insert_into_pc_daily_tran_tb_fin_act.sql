INSERT INTO
  `@curated_project.gwpc.pc_daily_transaction_staging` (
  SELECT
    CAST('@execution_date' AS DATETIME) AS dlh_batch_ts,
    DATETIME(TIMESTAMP(FORMAT_DATETIME("%Y-%m-%d %H:%M:%S",CURRENT_DATETIME("America/Toronto")))) AS dlh_process_ts,
    public_id,
  FROM (
    SELECT
      public_id,
    FROM
      `@curated_project.gwpc.policy_period`
  WHERE date(dlh_batch_ts) = '@execution_date' ) 
);