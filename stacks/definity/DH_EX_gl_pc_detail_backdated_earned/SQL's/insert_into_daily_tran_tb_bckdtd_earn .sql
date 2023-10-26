CREATE OR REPLACE TABLE
  `@derived_project.product_gl.effective_trans_staging` ( policy_period_public_id STRING,
    reported_producer_code STRING,
    rnk INT64 ) ;
-------------------------------------------------------------------------------------------------
INSERT INTO
  `@derived_project.product_gl.effective_trans_staging` (policy_period_public_id,
    reported_producer_code,
    rnk)
SELECT
  pp.public_id AS policy_period_public_id,
  COALESCE(eff_dt.producer_code, recent_tran.prod_cos_code, eff_cur.producer_code, pp.prod_cos_code) AS reported_producer_code,
  ROW_NUMBER() OVER (PARTITION BY pp.public_id ORDER BY recent_tran.model_number DESC) AS rnk
FROM (
  SELECT
    DISTINCT pp2.public_id
  FROM
    `@curated_project.gwpc.pc_daily_transaction_staging` AS dlytrn1
  INNER JOIN
    `@curated_project.gwpc.policy_period` AS pp_0
  ON
    dlytrn1.public_id = pp_0.public_id
  INNER JOIN
    `@curated_project.gwpc.policy_period` AS pp2
  ON
    pp_0.policy_number = pp2.policy_number
    AND pp_0.term_number = pp2.term_number ) AS dlytrn1
INNER JOIN
  `@curated_project.gwpc.policy_period` AS pp
ON
  dlytrn1.public_id = pp.public_id
LEFT OUTER JOIN
  `@curated_project.gwpc.policy_period` AS recent_tran
ON
  pp.policy_number = recent_tran.policy_number
  AND pp.term_number = recent_tran.term_number
  AND recent_tran.model_number > pp.model_number
  AND DATE(GREATEST(recent_tran.edit_effective_date, recent_tran.model_date)) <= DATE(pp.edit_effective_date)
LEFT OUTER JOIN
  `@curated_project.gwpc.eff_dated_fields` AS eff_dt
ON
  recent_tran.public_id = eff_dt.policy_period_public_id
  AND recent_tran.eff_dated_fields_public_id = eff_dt.public_id
LEFT OUTER JOIN
  `@curated_project.gwpc.eff_dated_fields` AS eff_cur
ON
  pp.public_id = eff_cur.policy_period_public_id
  AND pp.eff_dated_fields_public_id = eff_cur.public_id ;
-------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE
  `@derived_project.product_gl.bckdtd_trans_staging` ( policy_number STRING,
    term_number INT64,
    model_number INT64,
    public_id STRING,
    period_start DATETIME,
    period_end DATETIME,
    uw_company_code STRING,
    job_subtype STRING,
    broker_code STRING,
    prior_broker_code STRING,
    policy_period_public_id STRING,
    edit_effective_date DATETIME,
    prior_policy_period_public_id STRING,
    model_date DATETIME,
    job_close_date DATETIME,
    transaction_costrpt_amt NUMERIC(33,
      4) ) ;
----------------------------------------------------------------------------------------------------
INSERT INTO
  `@derived_project.product_gl.bckdtd_trans_staging` (policy_number,
    term_number,
    model_number,
    public_id,
    period_start,
    period_end,
    uw_company_code,
    job_subtype,
    broker_code,
    prior_broker_code,
    policy_period_public_id,
    edit_effective_date,
    prior_policy_period_public_id,
    model_date,
    job_close_date,
    transaction_costrpt_amt)
SELECT
  bto.policy_number AS policy_number,
  bto.term_number AS term_number,
  bto.model_number AS model_number,
  bto.public_id AS public_id,
  bto.period_start AS period_start,
  bto.period_end AS period_end,
  bto.uw_company_code AS uw_company_code,
  bto.job_subtype AS job_subtype,
  bto.broker_code,
  bto.prior_broker_code,
  bto.policy_period_public_id AS policy_period_public_id,
  bto.edit_effective_date AS edit_effective_date,
  bto.prior_policy_period_public_id AS prior_policy_period_public_id,
  bto.model_date AS model_date,
  bto.job_close_date AS job_close_date,
  bto.transaction_costrpt_amt AS transaction_costrpt_amt
FROM (
  SELECT
    pp.policy_number,
    pp.term_number,
    pp.model_number,
    pp.public_id,
    pp.period_start,
    pp.period_end,
    pp.uw_company_code,
    job.subtype AS job_subtype,
    eff1.reported_producer_code AS broker_code,
    eff.reported_producer_code AS prior_broker_code,
    pp.public_id AS policy_period_public_id,
    pp.edit_effective_date,
    ppv.public_id AS prior_policy_period_public_id,
    pp.model_date,
    job.close_date AS job_close_date,
    pp.transaction_costrpt_amt,
    ROW_NUMBER() OVER (PARTITION BY pp.public_id, pp.policy_number, pp.term_number ORDER BY ppv.model_number DESC) AS rnk
  FROM
    `@curated_project.gwpc.pc_daily_transaction_staging` AS dlytrn
  INNER JOIN
    `@curated_project.gwpc.policy_period` AS pp
  ON
    dlytrn.public_id = pp.public_id
  INNER JOIN
    `@curated_project.gwpc.job` job
  ON
    pp.public_id = job.policy_period_public_id
  LEFT OUTER JOIN
    `@curated_project.gwpc.policy_period` AS ppv
  ON
    pp.policy_number = ppv.policy_number
    AND pp.term_number = ppv.term_number
    AND pp.model_number > ppv.model_number
    AND job.subtype <> 'Reinstatement'
    AND DATE(ppv.edit_effective_date) <= DATE(pp.model_date)
  LEFT OUTER JOIN
    `@derived_project.product_gl.effective_trans_staging` AS eff1
  ON
    pp.public_id = eff1.policy_period_public_id
    AND eff1.rnk = 1
  LEFT OUTER JOIN
    `@derived_project.product_gl.effective_trans_staging` AS eff
  ON
    COALESCE(ppv.public_id, pp.public_id) = eff.policy_period_public_id
    AND eff.rnk = 1
  WHERE
    DATE(pp.edit_effective_date) < DATE(pp.model_date) ) AS bto
WHERE
  bto.rnk = 1
  AND (bto.broker_code <> bto.prior_broker_code
    OR ABS(bto.transaction_costrpt_amt) > NUMERIC '0.00') ;