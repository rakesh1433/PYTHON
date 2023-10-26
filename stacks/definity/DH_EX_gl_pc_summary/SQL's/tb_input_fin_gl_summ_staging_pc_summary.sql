DECLARE
  precison INT64 DEFAULT 2;
DECLARE
  accident_year String DEFAULT '0';
DECLARE
  reinsurance_treaty String DEFAULT '0';
DECLARE
  PROJECT String DEFAULT '0';
DECLARE
  future String DEFAULT '0';
  INSERT INTO
  `@derived_project.product_gl.finance_gl_summary` (
    SELECT
    CAST('@execution_date' AS DATETIME) AS dlh_batch_ts,
      DATETIME(TIMESTAMP(FORMAT_DATETIME("%Y-%m-%dT%H:%M:%S",CURRENT_DATETIME("America/Toronto")))) AS dlh_process_ts,				
mon.amount_category,	
mon.accounting_date as accounting_date ,
mon.uw_company_code,
mon.gl_account_number,
CASE
        WHEN mon.uw_company_code = '01' AND mon.branch_number = '1' THEN COALESCE(branch.gl_branch_number, '31')
      ELSE
      mon.branch_number
    END
      AS branch_number,
mon.gl_function,	
mon.product_code,
mon.product_type,	
mon.product_desc,
mon.class_of_business_code,
mon.coverable_province,
mon.accident_year,
mon.reinsurance_treaty,
mon.producer_code_of_record_code,
mon.PROJECT,
mon.future,
ROUND(SUM(mon.amount_amt), precison) AS sum_amount_amt_rounded,
mon.accounting_entry_type,
mon.amount_sub_category,
mon.external_lob_indicator,
EXTRACT(YEAR
FROM
accounting_date) AS accounting_date_year,
EXTRACT(MONTH
FROM
accounting_date) AS accounting_date_month,
    FROM
      `@derived_project.product_gl.finance_gl_detail_monthly` AS mon
    LEFT OUTER JOIN (
      SELECT
        agent_master.co_cd,
        agent_master.brk_num,
        agent_master.gw_brk_num,
        agent_master.branch_number AS branch_number,
        agent_master.insp_num,
        ROW_NUMBER() OVER (PARTITION BY agent_master.gw_brk_num ORDER BY agent_master.processed_date DESC) AS sel
      FROM
        `@curated_project.agnt.agent_master` agent_master ) AS agnt
    ON
      agnt.sel = 1
      AND agnt.gw_brk_num = mon.producer_code_of_record_code
    LEFT OUTER JOIN
      `@derived_project.product_gl.finance_gl_branch` AS branch
    ON
      branch.company_code = agnt.co_cd
      AND branch.branch_number = agnt.branch_number
    WHERE
      DATE(accounting_date) BETWEEN CAST('@summary_startdate' AS DATE)
      AND CAST('@summary_enddate' AS DATE)
    GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      15,
      16,
      17,
      18,
      20,
      mon.amount_sub_category,
      mon.external_lob_indicator,
      22
      order by
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      15,
      16,
      17,
      18,
      20,
      mon.amount_sub_category,
      mon.external_lob_indicator,
      22)
     ;

