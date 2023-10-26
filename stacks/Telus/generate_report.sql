drop TABLE CHINATEL_W_BILL_SEQ_NO;

drop table CHINATEL_W_SUBSCRIBER_BAN;

drop table CHINATEL_W_SERVICE_AGREEMENT;

drop table CHINATEL_W_SERVICE_AGRMNT_V2;

drop table CHINATEL_W_PROMO_PP_HISTORY;

drop table CHINATEL_W_PROMO_HISTORY;

drop table CHINATEL_W_PROMO_NO_FILTER;

drop table CHINATEL_T_PROMO_SUMMARY;


-- step 1
create TABLE CHINATEL_W_BILL_SEQ_NO
AS
select SUBSCRIBER_NO , BAN, BILL_SEQ_NO as LAST_BILL_SEQ_NO, BILL_DATE
FROM (
    select /*+ PARALLEL(a,16)*/ SUBSCRIBER_NO, BAN, --ROW_NUMBER() OVER (PARTITION BY SUBSCRIBER_NO ORDER BY SYS_CREATION_DATE DESC) as ROW_VALUE,
        BILL_SEQ_NO, TRUNC(SYS_CREATION_DATE,'MM') as BILL_DATE
    from bill_subscriber@pol1_app a where BAN IN (33824591, 34128154, 36238435, 36238478, 36780322, 36780335, 36780343, 36780351)
) 
-- bill month for the previous month. If this is for June bill , input July as the bill date.
-- *************** CHANGE HERE 
where BILL_DATE = TO_DATE('2023-06-01 00:00:00','YYYY-MM-DD HH24:MI:SS');

-- STEP 2 
CREATE TABLE CHINATEL_W_SUBSCRIBER_BAN
AS
WITH 
SUBSCRIBER_BAN_V1 AS ( 
select subscriber_no,LATEST_BAN, CAST (LATEST_BAN_ACTV_DATE as DATE) as LATEST_BAN_ACTV_DATE, PREV_BAN , TRIM(PROVINCE) as PROVINCE
from (
    -- Step 0 : This gets all the subscriber phone numbers and BANs associated. Get the latest BAN to run the promo along with the PREV_BAN to be able to identify BAN switch.
    select /*+ PARALLEL(a,16)*/ subscriber_no,CUSTOMER_ID as LATEST_BAN, INIT_ACTIVATION_DATE as LATEST_BAN_ACTV_DATE, PRV_BAN as PREV_BAN, 
    CASE 
    WHEN b.SUB_MARKET_CODE = 'CAL' THEN 'AB'
    ELSE b.SUB_MARKET_CODE
    END AS PROVINCE,
    ROW_NUMBER() OVER (PARTITION BY SUBSCRIBER_NO ORDER BY INIT_ACTIVATION_DATE desc) as ROW_VALUE 
        from subscriber@pol1_app a
            LEFT OUTER JOIN 
        (select * from MARKET_NPA_NXX_LR@pol1_app where product_type = 'C' and NVL(expiration_date,sysdate+1) >sysdate) b
        on SUBSTR(a.SUBSCRIBER_NO,1,3) = b.NPA
        and SUBSTR(a.SUBSCRIBER_NO,4,3) = b.NXX
        and SUBSTR(a.SUBSCRIBER_NO,7,4) BETWEEN b.BEGIN_LINE_RANGE and b.END_LINE_RANGE
        where CUSTOMER_ID IN (33824591, 34128154, 36238435, 36238478, 36780322, 36780335, 36780343, 36780351)
        --and subscriber_no IN ('9023888008','5142646922','2369690828','7807082845','6479757300')
    ) 
    where ROW_VALUE = 1
),
SUBSCRIBER_BAN AS ( -- THis gets the Prev BAN's activation date.
    select /*+ PARALLEL(a,16) PARALLEL(b,16)*/ a.*, b.INIT_ACTIVATION_DATE as  PREV_BAN_ACTV_DATE
        from SUBSCRIBER_BAN_V1 a LEFT OUTER JOIN subscriber@pol1_app b
        on a.PREV_BAN = b.CUSTOMER_ID
        and a.subscriber_no = b.subscriber_no
        and a.LATEST_BAN = b.NEXT_BAN
        -- Checking Prev BAN for CT just to make sure there is no NON-CT BAN movement. or They maynot be having a prev BAN.
        where (a.PREV_BAN IN (33824591, 34128154, 36238435, 36238478, 36780322, 36780335, 36780343, 36780351) or a.PREV_BAN IS NULL)
)
select * from SUBSCRIBER_BAN;

-- STEP 3
-- Get all Priceplans for all CT Subscribers.
CREATE TABLE CHINATEL_W_SERVICE_AGREEMENT
AS
select * from service_agreement@pol1_app 
where customer_id IN (33824591, 34128154, 36238435, 36238478, 36780322, 36780335, 36780343, 36780351)
and service_type='P' ;


-- STEP 4
CREATE TABLE CHINATEL_W_SERVICE_AGRMNT_V2
AS
WITH
SERVICE_AGREEMENT_V 
-- This get all the Priceplan SOCs for the subscribers in the order in which they were activated or changed.
AS (
SELECT * FROM  
-- If it is an active SOC, expiration date will be NULL, so it is set to be expiring sysdate+1 for calculation purposes.
(   select /*+ PARALLEL(a,16) PARALLEL(b,16)*/  a.subscriber_no, a.BAN, soc, EFFECTIVE_DATE, NVL(EXPIRATION_DATE,SYSDATE+1) EXPIRATION_DATE, SYS_CREATION_DATE, LATEST_BAN_ACTV_DATE,PREV_BAN,PREV_BAN_ACTV_DATE,PROVINCE
    from CHINATEL_W_SERVICE_AGREEMENT a 
    INNER JOIN CHINATEL_W_SUBSCRIBER_BAN b
        ON a.BAN IN ( NVL(b.PREV_BAN, b.LATEST_BAN) , b.LATEST_BAN)
        and a.SUBSCRIBER_NO = b.subscriber_no
        and a.EFFECTIVE_DATE >= NVL(b.PREV_BAN_ACTV_DATE,b.LATEST_BAN_ACTV_DATE) -- Adding to consider the Priceplans from where the customer moved from.  
) tbl1
MATCH_RECOGNIZE ( -- Match SOC and compress them into few record if the current and the next row has the same SOC with the continuing dates
  PARTITION BY subscriber_no
  ORDER BY EFFECTIVE_DATE, SYS_CREATION_DATE
  MEASURES
    LAST( BAN )    AS BAN,
    FIRST( LATEST_BAN_ACTV_DATE )    AS LATEST_BAN_ACTV_DATE,
    FIRST( PREV_BAN )    AS PREV_BAN,
    FIRST( PREV_BAN_ACTV_DATE ) as PREV_BAN_ACTV_DATE,
    FIRST( TRIM(SOC) )    AS SOC,
    FIRST( EFFECTIVE_DATE ) AS SOC_EFFECTIVE_DATE,
    LAST( EXPIRATION_DATE ) AS EXPIRATION_DATE,
    FIRST( SYS_CREATION_DATE ) as SYS_CREATION_DATE,
    FIRST (PROVINCE) as PROVINCE
  PATTERN ( same_soc + )
  DEFINE same_soc AS FIRST(TRIM(soc)) = TRIM(soc))
),
SERVICE_AGREEMENT_V2 AS (
    -- This will arrange the SOCs in the order to identify the first and next SOC to replace the updated SOCs in the next level of the que
    select subscriber_no, BAN, LATEST_BAN_ACTV_DATE,PREV_BAN,PREV_BAN_ACTV_DATE, TRIM(SOC) AS SOC, SOC_EFFECTIVE_DATE, EXPIRATION_DATE, SYS_CREATION_DATE,PROVINCE,
    ROW_NUMBER() OVER (PARTITION BY subscriber_no ORDER BY SOC_EFFECTIVE_DATE , SYS_CREATION_DATE) SOC_ORDER 
    from SERVICE_AGREEMENT_V
)
select * from SERVICE_AGREEMENT_V2;


-- STEP 5
CREATE TABLE CHINATEL_W_PROMO_PP_HISTORY
as
WITH
SERVICE_AGRMNT_V2_FIRST_SOC AS (
-- This will only get the FIRST SOCs for all subscribers to be able to use in the below V3 subquery
select * from CHINATEL_W_SERVICE_AGRMNT_V2 where SOC_ORDER=1
),
-- This is the version of SERVICE_AGREEMENT_V3 prior to updating the logic for replacing new SOCs with Old SOCs to keep the promo running for customers.
SERVICE_AGREEMENT_V3 as 
(
    select * from (
        select  SUBSCRIBER_NO, BAN, LATEST_BAN_ACTV_DATE, PREV_BAN, PREV_BAN_ACTV_DATE, ORIGINAL_SOC, SOC, SOC_EFFECTIVE_DATE, EXPIRATION_DATE, SYS_CREATION_DATE, SOC_ORDER, SOC_REPLACED_IND,PROVINCE,
              ROW_NUMBER() OVER (PARTITION BY SUBSCRIBER_NO, SOC_ORDER ORDER BY SOC_ORDER,SOC_REPLACED_IND DESC) as ROW_VALUE
        from (
            select /*+ PARALLEL(a,8) */  a.subscriber_no, a.BAN, a.LATEST_BAN_ACTV_DATE,a.PREV_BAN, a.PREV_BAN_ACTV_DATE,TRIM(a.SOC) as ORIGINAL_SOC, a.PROVINCE,
            -- Look for the new price plan that needs to be replaced to the FIRST SOC and make sure that the match is only to the one record the customer signed up with. 
                    CASE WHEN (TRIM(a.SOC) = TRIM(b.NEW_PRICE_PLAN_CD)  and a.SOC_ORDER>1) 
                      and TRIM(c.SOC) = TRIM(b.ORIG_PRICE_PLAN_CD)
                    THEN TRIM(b.ORIG_PRICE_PLAN_CD)
                        ELSE TRIM(a.SOC)
                    END AS SOC, a.SOC_EFFECTIVE_DATE, a.EXPIRATION_DATE, a.SYS_CREATION_DATE, a.SOC_ORDER ,
                    -- This is to mark which records are replaced with the old SOC since the new SOCs were introduced.
                    CASE WHEN (TRIM(a.SOC) = TRIM(b.NEW_PRICE_PLAN_CD)  and a.SOC_ORDER>1) 
                      and TRIM(c.SOC) = TRIM(b.ORIG_PRICE_PLAN_CD)
                    THEN 1
                        ELSE 0
                    END AS SOC_REPLACED_IND
            from CHINATEL_W_SERVICE_AGRMNT_V2 a 
            LEFT OUTER JOIN dat.CHINATEL_R_PP_MIGRATION b
                on TRIM(a.SOC) = TRIM(b.NEW_PRICE_PLAN_CD)
            LEFT OUTER JOIN SERVICE_AGRMNT_V2_FIRST_SOC c
                ON a.subscriber_no = c.subscriber_no
        )
    ) where ROW_VALUE=1
)--select * from SERVICE_AGREEMENT_V3
,
SERVICE_AGREEMENT_V4 as ( -- Merge the replaced entries again into ONE.
    SELECT * FROM  
    (select subscriber_no, BAN, LATEST_BAN_ACTV_DATE, PREV_BAN, PREV_BAN_ACTV_DATE, original_soc, soc, SOC_EFFECTIVE_DATE, EXPIRATION_DATE, SYS_CREATION_DATE,PROVINCE
            from  SERVICE_AGREEMENT_V3
            group by subscriber_no, BAN, LATEST_BAN_ACTV_DATE, PREV_BAN, PREV_BAN_ACTV_DATE, original_soc, soc, SOC_EFFECTIVE_DATE, EXPIRATION_DATE, SYS_CREATION_DATE,PROVINCE
            ) tbl1
    MATCH_RECOGNIZE (
      PARTITION BY subscriber_no
      ORDER BY SOC_EFFECTIVE_DATE, SYS_CREATION_DATE
      MEASURES
        LAST( TRIM(BAN) )    AS BAN, 
        FIRST( TRIM(LATEST_BAN_ACTV_DATE) )    AS LATEST_BAN_ACTV_DATE,
        FIRST( TRIM(PREV_BAN) )    AS PREV_BAN,
        FIRST( PREV_BAN_ACTV_DATE ) AS PREV_BAN_ACTV_DATE,
        LAST( TRIM(ORIGINAL_SOC) )    AS ORIGINAL_SOC,
        FIRST( TRIM(SOC) )    AS SOC,
        FIRST( SOC_EFFECTIVE_DATE ) AS SOC_EFFECTIVE_DATE,
        LAST( EXPIRATION_DATE ) AS EXPIRATION_DATE,
        FIRST( SYS_CREATION_DATE ) as SYS_CREATION_DATE,
        FIRST (PROVINCE) as PROVINCE
      PATTERN ( same_soc + )
      DEFINE same_soc AS FIRST(TRIM(soc)) = TRIM(soc))
),
SERVICE_AGREEMENT_V1 AS (
select ban, subscriber_no, LATEST_BAN_ACTV_DATE, PREV_BAN, PREV_BAN_ACTV_DATE, PROVINCE, MAX(SOC) as SOC, MAX(ORIGINAL_SOC) ORIGINAL_SOC, MAX(SOC_EFFECTIVE_DATE) as SOC_EFFECTIVE_DATE, MAX(EXPIRATION_DATE) EXPIRATION_DATE,
                  MAX(NEXT_SOC) as NEXT_SOC, MAX(NEXT_ORIGINAL_SOC) as NEXT_ORIGINAL_SOC, MAX(NEXT_SOC_EFF_DATE) as NEXT_SOC_EFF_DATE, MAX(NEXT_SOC_EXP_DATE) as NEXT_SOC_EXP_DATE,
                  MAX(NEXT_TWO_SOC) as NEXT_TWO_SOC, MAX(NEXT_TWO_ORIG_SOC) as NEXT_TWO_ORIG_SOC ,MAX(NEXT_TWO_SOC_EFF_DATE) as NEXT_TWO_SOC_EFF_DATE, MAX(NEXT_TWO_SOC_EXP_DATE) as NEXT_TWO_SOC_EXP_DATE
from (
    select ban, subscriber_no, LATEST_BAN_ACTV_DATE, PREV_BAN, PREV_BAN_ACTV_DATE,PROVINCE,
            CASE WHEN SOC_ORDER=1 THEN SOC  ELSE NULL END AS SOC, 
            CASE WHEN SOC_ORDER=1 THEN ORIGINAL_SOC  ELSE NULL END AS ORIGINAL_SOC,  
            CASE WHEN SOC_ORDER=1 THEN SOC_EFFECTIVE_DATE  ELSE NULL END AS SOC_EFFECTIVE_DATE,
            CASE WHEN SOC_ORDER=1 THEN EXPIRATION_DATE  ELSE NULL END AS EXPIRATION_DATE,
            CASE WHEN SOC_ORDER=2 THEN SOC  ELSE NULL END AS NEXT_SOC, 
            CASE WHEN SOC_ORDER=2 THEN ORIGINAL_SOC  ELSE NULL END AS NEXT_ORIGINAL_SOC, 
            CASE WHEN SOC_ORDER=2 THEN SOC_EFFECTIVE_DATE  ELSE NULL END AS NEXT_SOC_EFF_DATE,
            CASE WHEN SOC_ORDER=2 THEN EXPIRATION_DATE  ELSE NULL END AS NEXT_SOC_EXP_DATE,
            CASE WHEN SOC_ORDER=3 THEN SOC  ELSE NULL END AS NEXT_TWO_SOC, 
            CASE WHEN SOC_ORDER=3 THEN ORIGINAL_SOC  ELSE NULL END AS NEXT_TWO_ORIG_SOC,
            CASE WHEN SOC_ORDER=3 THEN SOC_EFFECTIVE_DATE  ELSE NULL END AS NEXT_TWO_SOC_EFF_DATE,
            CASE WHEN SOC_ORDER=3 THEN EXPIRATION_DATE  ELSE NULL END AS NEXT_TWO_SOC_EXP_DATE
        from (
            select * from (
                select a.subscriber_no, a.ban, LATEST_BAN_ACTV_DATE, PREV_BAN, PREV_BAN_ACTV_DATE, ORIGINAL_SOC, SOC, SOC_EFFECTIVE_DATE, EXPIRATION_DATE, SYS_CREATION_DATE,
                ROW_NUMBER() OVER (PARTITION BY a.subscriber_no ORDER BY SOC_EFFECTIVE_DATE , SYS_CREATION_DATE) SOC_ORDER, PROVINCE
                from SERVICE_AGREEMENT_V4 a
            ) where SOC_ORDER IN (1,2,3) -- only take the 3 SOC history
    )
) group by ban, subscriber_no, LATEST_BAN_ACTV_DATE, PREV_BAN, PREV_BAN_ACTV_DATE,PROVINCE
),
PRICEPLAN_HISTORY AS (
    select BAN, a.SUBSCRIBER_NO,LATEST_BAN_ACTV_DATE, PREV_BAN, PREV_BAN_ACTV_DATE,
            SOC,SOC_EFFECTIVE_DATE,
            ORIGINAL_SOC, NEXT_ORIGINAL_SOC, NEXT_TWO_ORIG_SOC,
            CASE WHEN (TRUNC(NEXT_SOC_EFF_DATE) - TRUNC(EXPIRATION_DATE))> 0  THEN NEXT_SOC_EFF_DATE
                ELSE EXPIRATION_DATE
            END AS EXPIRATION_DATE,
            NEXT_SOC,NEXT_SOC_EFF_DATE,NEXT_SOC_EXP_DATE,
            NEXT_TWO_SOC,NEXT_TWO_SOC_EFF_DATE,NEXT_TWO_SOC_EXP_DATE,
            CASE WHEN (TRUNC(NEXT_SOC_EFF_DATE) - TRUNC(EXPIRATION_DATE))> 0 THEN (TRUNC(NEXT_SOC_EFF_DATE) - TRUNC(SOC_EFFECTIVE_DATE))
                ELSE (TRUNC(EXPIRATION_DATE) - TRUNC(SOC_EFFECTIVE_DATE))
            END AS DAYS_IN_CURRENT_SOC, PROVINCE
    from SERVICE_AGREEMENT_V1 a
)
select * from PRICEPLAN_HISTORY
;

-- STEP 6
create table CHINATEL_W_PROMO_HISTORY
As
WITH
PROMO_HISTORY as 
( -- Step 4 - Apply business logic from Step 3 results.
select a.BAN, a.SUBSCRIBER_NO, a.SOC, a.SOC_EFFECTIVE_DATE, 
                               ORIGINAL_SOC, NEXT_ORIGINAL_SOC, NEXT_TWO_ORIG_SOC,
                               a.EXPIRATION_DATE,b.GRACE_PERIOD_DATE, 
                               a.NEXT_SOC, a.NEXT_SOC_EFF_DATE, a.NEXT_SOC_EXP_DATE,
                               a.NEXT_TWO_SOC,a.NEXT_TWO_SOC_EFF_DATE,a.NEXT_TWO_SOC_EXP_DATE,
        NVL(b.START_DATE,c.START_DATE) START_DATE, NVL(b.END_DATE,c.END_DATE) END_DATE, NVL(b.CAMPAIGN_NOTE,c.CAMPAIGN_NOTE) CAMPAIGN_NOTE, 
        -- Condition 4.1 - After joining with the campaign date ranges, any NULL campaign_note values mean the subscriber PP entry is not eligible for the promo 
        CASE WHEN NVL(b.CAMPAIGN_NOTE,c.CAMPAIGN_NOTE) IS NULL THEN 'NON-ELIGIBLE' ELSE 'ELIGIBLE' END as ELIGIBLE_IND,
        NVL(b.MONTHS_ELIGIBLE,c.MONTHS_ELIGIBLE) MONTHS_ELIGIBLE,
        NVL(b.PROMO_AMT,c.PROMO_AMT)  PROMO_AMT,
        NVL(b.PROMO_AMT,c.PROMO_AMT) * NVL(b.MONTHS_ELIGIBLE,c.MONTHS_ELIGIBLE) as PROJECTED_FULL_CREDIT_AMT,
        -- We take only 3 SOC history to perform the whole analysis, if the customer had more than 3 PP history it will not accounted for.
        -- MONTHS_INTO_PROMO - calculates how many months have the subscriber held the PP and accordingly the promo amt will be calculated.
        -- Condition 4.2 - if second soc is vacation PP, 1 and 3rd are the same PP along with 
            -- the days of the vacation PP is between 0 and 30 days then the third soc's expiration should be considered.  
        CASE
            WHEN a.NEXT_SOC IN ('XPCT0VSU','XPOTHSER0','VAD15') and a.SOC = a.NEXT_TWO_SOC and (a.NEXT_SOC_EXP_DATE - NEXT_SOC_EFF_DATE) BETWEEN 0 and 30
                THEN CEIL((MONTHS_BETWEEN(a.NEXT_TWO_SOC_EXP_DATE,a.SOC_EFFECTIVE_DATE)))
            -- There maybe changes in the priceplan but it might be the same SOC 3 times. It does not cover more than 3 time changes.
            WHEN a.SOC = a.NEXT_SOC and a.NEXT_SOC = a.NEXT_TWO_SOC
                THEN CEIL((MONTHS_BETWEEN(a.NEXT_TWO_SOC_EXP_DATE,a.SOC_EFFECTIVE_DATE)))
            -- Condition 4.3 - If the 1st and 2nd SOC are the same PP then take the 2nd SOC expiration date.   
            WHEN a.SOC = a.NEXT_SOC  
                THEN CEIL((MONTHS_BETWEEN(a.NEXT_SOC_EXP_DATE,a.SOC_EFFECTIVE_DATE)))
            ELSE
            -- Condition 4.4 - Default is take the first SOC's expiration date. 
                CEIL((MONTHS_BETWEEN(a.EXPIRATION_DATE,a.SOC_EFFECTIVE_DATE)))
        END as MONTHS_INTO_PROMO,
        CASE
            WHEN a.NEXT_SOC IN ('XPCT0VSU','XPOTHSER0','VAD15') and a.SOC = a.NEXT_TWO_SOC and (a.NEXT_SOC_EXP_DATE - NEXT_SOC_EFF_DATE) BETWEEN 0 and 30
                THEN TRUNC(a.NEXT_TWO_SOC_EXP_DATE) - TRUNC(a.SOC_EFFECTIVE_DATE)
            -- There maybe changes in the priceplan but it might be the same SOC 3 times. It does not cover more than 3 time changes.
            WHEN a.SOC = a.NEXT_SOC and a.NEXT_SOC = a.NEXT_TWO_SOC
                THEN TRUNC(a.NEXT_TWO_SOC_EXP_DATE) - TRUNC(a.SOC_EFFECTIVE_DATE)
            -- Condition 4.3 - If the 1st and 2nd SOC are the same PP then take the 2nd SOC expiration date.   
            WHEN a.SOC = a.NEXT_SOC  
                THEN TRUNC(a.NEXT_SOC_EXP_DATE) - TRUNC(a.SOC_EFFECTIVE_DATE)
            ELSE
            -- Condition 4.4 - Default is take the first SOC's expiration date. 
                TRUNC(a.EXPIRATION_DATE) - TRUNC(a.SOC_EFFECTIVE_DATE)
        END as DAYS_INTO_PROMO,
        -- MODIFIED_EXP_DATE column is a re-calculated field to account for Vacation PP logic similar to Conditions 4.2, 4.3, 4.4 in the MONTHS_INTO_PROMO field.
        CASE 
        WHEN a.NEXT_SOC IN ('XPCT0VSU','XPOTHSER0','VAD15') and a.SOC = a.NEXT_TWO_SOC and (a.NEXT_SOC_EXP_DATE - NEXT_SOC_EFF_DATE) BETWEEN 0 and 30
            THEN a.NEXT_TWO_SOC_EXP_DATE
        WHEN a.SOC = a.NEXT_SOC and a.NEXT_SOC = a.NEXT_TWO_SOC
            THEN a.NEXT_TWO_SOC_EXP_DATE
        WHEN a.SOC = a.NEXT_SOC 
            THEN a.NEXT_SOC_EXP_DATE
        ELSE 
            a.EXPIRATION_DATE
        END as MODIFIED_EXP_DATE,
        -- TIME_TO_VACATION_SOC column is a calculated field to account for Vacation PP duration 
        CASE 
            -- Condition 5.1 - If the 1st SOC is not vacation and the 2nd is vacation then count the days for the 1st SOC
            WHEN a.SOC NOT IN ('XPCT0VSU','XPOTHSER0','VAD15') and  a.NEXT_SOC IN ('XPCT0VSU','XPOTHSER0','VAD15') 
                THEN DAYS_IN_CURRENT_SOC
            -- Condition 5.2 - If the 1st SOC and 2nd SOC are same and the 3rd SOC is a vacation then count the days between the 1st and 2nd SOC
            WHEN a.SOC = a.NEXT_SOC and NEXT_TWO_SOC IN ('XPCT0VSU','XPOTHSER0','VAD15') 
                THEN a.NEXT_SOC_EXP_DATE - a.SOC_EFFECTIVE_DATE
            ELSE -1 END  as TIME_TO_VACATION_SOC,
        -- DAYS_OUT_OF_VACATION - field is calculated to find how many days the subscriber stayed in vacation before moving to the same PP as before.
        CASE  -- Condition 6.1 - If the 2nd SOC is a vacation, 1st and 3rd are the same then count the days for the 2nd SOC which is the vacation.
            WHEN a.NEXT_SOC IN ('XPCT0VSU','XPOTHSER0','VAD15') and  a.NEXT_TWO_SOC = a.SOC 
                THEN (NEXT_SOC_EXP_DATE - NEXT_SOC_EFF_DATE)
            -- Condition 6.2 - If the 3rd SOC is a vacation, 1st and 2nd are the same then count the days for the 3rd SOC which is the vacation.
            WHEN a.NEXT_TWO_SOC IN ('XPCT0VSU','XPOTHSER0','VAD15') and  a.NEXT_SOC = a.SOC 
                THEN (NEXT_TWO_SOC_EXP_DATE - NEXT_TWO_SOC_EFF_DATE)
            ELSE -1 
        END  as DAYS_OUT_OF_VACATION,
    CASE WHEN SOC_EFFECTIVE_DATE BETWEEN NVL(b.START_DATE, c.START_DATE) and NVL(b.END_DATE, c.END_DATE) 
        THEN 'FULL_WINDOW_Y'
        ELSE 'FULL_WINDOW_N'
    END AS FULL_WINDOW_IND,
    CASE WHEN SOC_EFFECTIVE_DATE > NVL(b.END_DATE, c.END_DATE) and SOC_EFFECTIVE_DATE <=NVL(b.GRACE_PERIOD_DATE, c.GRACE_PERIOD_DATE)
        THEN 'GRACE_WINDOW_Y'
        ELSE 'GRACE_WINDOW_N'
    END AS GRACE_WINDOW_IND,
    LATEST_BAN_ACTV_DATE, PREV_BAN, PREV_BAN_ACTV_DATE,PROVINCE
    from
    CHINATEL_W_PROMO_PP_HISTORY a
    -- Version 2 to 3 : Changed this from Left outer to FULL OUTER JOIN
    LEFT OUTER JOIN
    --Version 3 to 4 : Changed the full outer to left again. 
    --LEFT OUTER JOIN
    -- First check for all promos that does not have a check for subscriber no as well.
    (select * from dat.CHINATEL_R_PROMO_DETAIL where HAS_SUBS_FILTER_IND='N') b
        ON a.SOC_EFFECTIVE_DATE BETWEEN b.START_DATE and b.GRACE_PERIOD_DATE 
        and TRIM(a.SOC) = TRIM(b.PRICE_PLAN_CD)
    LEFT OUTER JOIN
    (select * from dat.CHINATEL_R_PROMO_DETAIL a 
        INNER JOIN DAT.CHINATEL_R_PROMO_SUBS b
        on a.SUBS_PROMO_ID = b.PROMO_DETAIL_ID
        where a.HAS_SUBS_FILTER_IND='Y'
        and b.PARAM_NM = 'PROVINCE'
     ) c
        ON a.SOC_EFFECTIVE_DATE BETWEEN c.START_DATE and c.GRACE_PERIOD_DATE 
        and TRIM(a.SOC) = TRIM(c.PRICE_PLAN_CD)
        and TRIM(a.PROVINCE) = TRIM(c.PARAM_TXT)
)
select * from PROMO_HISTORY where SUBSCRIBER_NO is not null
;


-- Step 7 .. This needs to be replicated for more logic.
create TABLE CHINATEL_W_PROMO_NO_FILTER 
AS
WITH
FLAG_ALL_ELIGIBLE AS ( -- Take all results from PROMO_HISTORY to apply more logic to recalcualte the eligibility criteria.
select a.* ,
-- Condition 8.3 Only take the first instance of the Promo eligible record.
CASE WHEN PROMO_EXP_ORDER>1 
THEN 'ELIGIBLE MULTI OFFER PERIOD'
ELSE ELIGIBLE_IND
END AS RECALC_ELIGIBLE_IND
from (
select 
a.BAN,a.SUBSCRIBER_NO, a.SOC,a.SOC_EFFECTIVE_DATE, ORIGINAL_SOC, NEXT_ORIGINAL_SOC, NEXT_TWO_ORIG_SOC,
a.EXPIRATION_DATE,a.MODIFIED_EXP_DATE,a.NEXT_SOC,a.NEXT_SOC_EFF_DATE,a.NEXT_SOC_EXP_DATE,START_DATE,END_DATE,
a.NEXT_TWO_SOC,a.NEXT_TWO_SOC_EFF_DATE,a.NEXT_TWO_SOC_EXP_DATE,
GRACE_PERIOD_DATE,CAMPAIGN_NOTE,
ELIGIBLE_IND,
-- Condition 8.1 Remove BAN as there are chances of BAN change for the same phone number. CT moved some customers from two bans to the other in March 2020.
ROW_NUMBER() OVER (PARTITION BY --a.BAN ,
                    a.subscriber_no, TRIM(SOC) ORDER BY a.SOC_EFFECTIVE_DATE, a.MODIFIED_EXP_DATE,START_DATE ASC) PROMO_EXP_ORDER,
MONTHS_ELIGIBLE,PROJECTED_FULL_CREDIT_AMT,PROMO_AMT,MONTHS_INTO_PROMO,
        CASE
            WHEN MONTHS_INTO_PROMO > MONTHS_ELIGIBLE THEN MONTHS_ELIGIBLE
            ELSE MONTHS_INTO_PROMO
        END as MOD_MONTHS_INTO_PROMO,
-- Condition 8.2 - If the MONTHS_INTO_PROMO is more than Promo period then set it to the promo period.
                -- This can happen if the customer stays in the same PP after the promo ended as well.
        CASE
            WHEN a.MONTHS_INTO_PROMO > MONTHS_ELIGIBLE
                THEN MONTHS_ELIGIBLE * a.PROMO_AMT
            ELSE a.MONTHS_INTO_PROMO * a.PROMO_AMT
        END as ELIGIBLE_CREDIT_TO_DATE,
        DAYS_INTO_PROMO,
FULL_WINDOW_IND,GRACE_WINDOW_IND,TIME_TO_VACATION_SOC, DAYS_OUT_OF_VACATION,
 LATEST_BAN_ACTV_DATE, PREV_BAN, PREV_BAN_ACTV_DATE, PROVINCE
from 
CHINATEL_W_PROMO_HISTORY a
-- added to eliminate any empty subscriber records since the promo history performs a FULL OUTER JOIN
where SUBSCRIBER_NO is not null
order by SOC_EFFECTIVE_DATE asc
) a
)
select * from FLAG_ALL_ELIGIBLE
;

--Step 8
CREATE TABLE CHINATEL_T_PROMO_SUMMARY
AS
WITH
ALL_ELIGIBLE_SUBSCRIBERS AS (
select * from (
-- get only the first eligible promo record, mark everything else as non-eligible
-- This can happen if the current promo ends and then the customer again signs up for a new promo.
-- This cannot happen since the promos only apply to new activations.
-- Rate plan change will not qualify for the second promo credit.
-- The promo credit only applies the first promo that is happening with the same SOC but at different Promo start dates.
select a.*, ROW_NUMBER() OVER (PARTITION BY a.subscriber_no ORDER BY a.SOC_EFFECTIVE_DATE,START_DATE ASC) PROMO_ORDER
    from CHINATEL_W_PROMO_NO_FILTER a --where RECALC_ELIGIBLE_IND = 'ELIGIBLE'
) where PROMO_ORDER = 1
),
MASTER_SUBSCRIBER_ELIGIBILITY AS (
select SUBSCRIBER_NO, INIT_ACTIVATION_DATE, BAN, SOC, SOC_EFFECTIVE_DATE,
      ORIGINAL_SOC, NEXT_ORIGINAL_SOC, NEXT_TWO_ORIG_SOC, 
      EXPIRATION_DATE, MODIFIED_EXP_DATE, 
      NEXT_SOC, NEXT_SOC_EFF_DATE, NEXT_SOC_EXP_DATE, START_DATE, END_DATE, NEXT_TWO_SOC, NEXT_TWO_SOC_EFF_DATE, NEXT_TWO_SOC_EXP_DATE, 
      GRACE_PERIOD_DATE, CAMPAIGN_NOTE, ELIGIBLE_IND, PROMO_EXP_ORDER, MONTHS_ELIGIBLE, PROJECTED_FULL_CREDIT_AMT, PROMO_AMT, MONTHS_INTO_PROMO,MOD_MONTHS_INTO_PROMO,
      DAYS_INTO_PROMO, ELIGIBLE_CREDIT_TO_DATE, FULL_WINDOW_IND, GRACE_WINDOW_IND, TIME_TO_VACATION_SOC, DAYS_OUT_OF_VACATION, 
      RECALC_ELIGIBLE_IND, PROMO_ORDER,
      CASE 
        -- Condition 9.1 - Activation of the 1set priceplan less than 14 days of opening the account are eligible for promo credit
        WHEN FINAL_ELIGIBILITY_IND = 'ELIGIBLE' and 
                (TRUNC(SOC_EFFECTIVE_DATE - INIT_ACTIVATION_DATE) <= 14) and
                DAYS_OUT_OF_VACATION BETWEEN -1 and 31 and -- Condition 9.2 - More than 31 days in vacation will disqualify promo credit
                --(TIME_TO_VACATION_SOC= -1 or TIME_TO_VACATION_SOC > 30 ) and -- Condition 9.3 - vacation cannot be within the first 31 days after activation, will disqualify
                DAYS_INTO_PROMO >= 28 
            THEN 'ELIGIBLE'
        -- Condition 9.4 - More than 14 days between initial activation and the first PP disqualifies.
        WHEN FINAL_ELIGIBILITY_IND = 'ELIGIBLE' and TRUNC(SOC_EFFECTIVE_DATE - INIT_ACTIVATION_DATE) > 14 THEN 'NON-ELIGIBLE-MORE-THAN-14DAY-DELAYED'
        WHEN FINAL_ELIGIBILITY_IND = 'ELIGIBLE' and DAYS_INTO_PROMO < 28 THEN 'NON-ELIGIBLE-EXPIRED-FIRST-28DAY'
        WHEN FINAL_ELIGIBILITY_IND = 'ELIGIBLE' and DAYS_OUT_OF_VACATION > 31 THEN 'ELIGIBLE-PARTIAL-MORE-THAN-31DAY-VACATION'
        --WHEN FINAL_ELIGIBILITY_IND = 'ELIGIBLE' and (TIME_TO_VACATION_SOC BETWEEN 0 and 30 ) THEN 'NON-ELIGIBLE-VACATION-WITHIN-30DAY'
        ELSE 'NON-ELIGIBLE-NO-PROMO-SUBS'
      END AS MASTER_ELIGIBILITY, LATEST_BAN, LATEST_BAN_ACTV_DATE, PREV_BAN, PREV_BAN_ACTV_DATE,
      CASE WHEN PREV_BAN IS NOT NULL AND LATEST_BAN_ACTV_DATE > SOC_EFFECTIVE_DATE and LATEST_BAN_ACTV_DATE >= TO_DATE('2020-12-01 00:00:00','YYYY-MM-DD HH24:MI:SS')
        THEN 'Y'
        ELSE 'N'
      END AS BAN_CHANGE_IND,
      CASE WHEN PREV_BAN IS NOT NULL AND LATEST_BAN_ACTV_DATE > SOC_EFFECTIVE_DATE and LATEST_BAN_ACTV_DATE >= TO_DATE('2020-12-01 00:00:00','YYYY-MM-DD HH24:MI:SS')
        THEN CEIL((MONTHS_BETWEEN(LATEST_BAN_ACTV_DATE,SOC_EFFECTIVE_DATE)))
        ELSE NULL
      END AS MONTHS_IN_FIRST_BAN,
      CEIL(LATEST_BAN_ACTV_DATE - SOC_EFFECTIVE_DATE) AS DAYS_TO_BAN_CHANGE, PROVINCE as PROVINCE_CD
from (
select NVL(b.PREV_BAN_ACTV_DATE,LATEST_BAN_ACTV_DATE)  as INIT_ACTIVATION_DATE, BAN, SUBSCRIBER_NO, SOC, SOC_EFFECTIVE_DATE, ORIGINAL_SOC, NEXT_ORIGINAL_SOC, NEXT_TWO_ORIG_SOC, 
    EXPIRATION_DATE, MODIFIED_EXP_DATE, NEXT_SOC, NEXT_SOC_EFF_DATE, NEXT_SOC_EXP_DATE, START_DATE, 
    END_DATE, NEXT_TWO_SOC, NEXT_TWO_SOC_EFF_DATE, NEXT_TWO_SOC_EXP_DATE, GRACE_PERIOD_DATE, CAMPAIGN_NOTE, ELIGIBLE_IND, PROMO_EXP_ORDER, MONTHS_ELIGIBLE, 
    PROJECTED_FULL_CREDIT_AMT, PROMO_AMT, MONTHS_INTO_PROMO,MOD_MONTHS_INTO_PROMO, DAYS_INTO_PROMO,
    -- Condition 9.5 - Eligible credit should be 0 if vacation soc is added within 30 days of initial account activation
        -- Also credit is 0 if the First soc activation is more than 14 days after initial activation
    CASE 
        WHEN RECALC_ELIGIBLE_IND = 'ELIGIBLE' and --((TIME_TO_VACATION_SOC BETWEEN 0 and 30) or 
                                    (TRUNC(SOC_EFFECTIVE_DATE - NVL(b.PREV_BAN_ACTV_DATE,LATEST_BAN_ACTV_DATE)) > 14)
                                        --)  
                                    THEN 0
        WHEN RECALC_ELIGIBLE_IND = 'ELIGIBLE' and DAYS_INTO_PROMO < 28   THEN 0
        ELSE ELIGIBLE_CREDIT_TO_DATE
    END AS ELIGIBLE_CREDIT_TO_DATE, 
    FULL_WINDOW_IND, GRACE_WINDOW_IND, TIME_TO_VACATION_SOC, DAYS_OUT_OF_VACATION, 
    RECALC_ELIGIBLE_IND, PROMO_ORDER ,
    -- Condition 9.6 - Mark all NULL eligible ind to Non-eligible for sanity sake.
        CASE 
            WHEN RECALC_ELIGIBLE_IND IS NOT NULL 
                THEN RECALC_ELIGIBLE_IND
            ELSE 'NON-ELIGIBLE'
        END as FINAL_ELIGIBILITY_IND,
        BAN AS LATEST_BAN, CAST (LATEST_BAN_ACTV_DATE as DATE) LATEST_BAN_ACTV_DATE, PREV_BAN, PREV_BAN_ACTV_DATE, PROVINCE
    from 
 ALL_ELIGIBLE_SUBSCRIBERS b 
)),
SUBSCRIBER_BILL AS (
-- Get all the credits for the subscriber since activation date.
select subscriber_no, NVL(SUM(CURR_CREDIT_AMT),0) AS TOTAL_BILL_CREDIT_AMT,NVL(SUM(TOTAL_BILLED_ADJUST),0) AS TOTAL_BILLED_ADJUST_AMT--, MIN(SYS_CREATION_DATE) as FIRST_BILL_DATE, MAX(SYS_CREATION_DATE) as LAST_BILL_DATE 
from bill_subscriber@pol1_app where BAN IN (33824591, 34128154, 36238435, 36238478, 36780322, 36780335, 36780343, 36780351)
group by subscriber_no
),
ADJUSTMENT_CREDIT_AMT AS (
select -- Get all adjustments to get the breakdown
SUBSCRIBER_NO, SUM(MISC_AMT) MISC_AMT, SUM(ADDON_CORRECTION_AMT) ADDON_CORRECTION_AMT,
               SUM(BILL_CREDIT_REVERSAL_AMT) as BILL_CREDIT_REVERSAL_AMT, 
               SUM(ADJ_REVERSAL_AMT) ADJ_REVERSAL_AMT, SUM(USG_CORRECTION_AMT) USG_CORRECTION_AMT,
               SUM(TAX_CREDIT_AMT) TAX_CREDIT_AMT, SUM(BILL_CREDIT_AMT) BILL_CREDIT_AMT, SUM(CONTRACT_SOC_BUILD_AMT) CONTRACT_SOC_BUILD_AMT,
               --SUM(PP_CHANGE_AMT) PP_CHANGE_AMT, 
               SUM(RATE_PLAN_CHANGE_AMT) RATE_PLAN_CHANGE_AMT, SUM(OTHER_CREDIT_AMT) OTHER_CREDIT_AMT, SUM(ACTV_AMT) TOTAL_ACTV_AMT
FROM (
select subscriber_no, a.ACTV_CODE ,a.ACTV_REASON_CODE,
        CASE WHEN TRIM(a.ACTV_REASON_CODE) IN ('EXCACR','ERRAPN','BASE','DISC','DISREL','SD0031','RAFRBC') THEN SUM(ACTV_AMT) ELSE 0 END AS MISC_AMT,
        CASE WHEN TRIM(a.ACTV_REASON_CODE) IN ('FTRESC') THEN SUM(ACTV_AMT) ELSE 0 END AS ADDON_CORRECTION_AMT,
        CASE WHEN TRIM(a.ACTV_REASON_CODE) IN ('ERROR') THEN SUM(ACTV_AMT) ELSE 0 END AS BILL_CREDIT_REVERSAL_AMT,
        CASE WHEN TRIM(a.ACTV_REASON_CODE) IN ('ACTCH') THEN SUM(ACTV_AMT) ELSE 0 END AS ADJ_REVERSAL_AMT,
        CASE WHEN TRIM(a.ACTV_REASON_CODE) IN ('AIRA','OGWVA','OGWDD','SMRTPH','LD','OGWVLD','RRVA','CVDRMW','AIRARO') THEN SUM(ACTV_AMT) ELSE 0 END AS USG_CORRECTION_AMT,
        CASE WHEN TRIM(a.ACTV_REASON_CODE) IN ('PSTB','HST','PSTM','PSTQ','TAXG','TAXRND','TAXSK') THEN SUM(ACTV_AMT) ELSE 0 END AS TAX_CREDIT_AMT,
        CASE WHEN TRIM(a.ACTV_REASON_CODE) IN ('BYOD24','RCADJ3','ENTCR','P2KRBC','RBC24M') THEN SUM(ACTV_AMT) ELSE 0 END AS BILL_CREDIT_AMT,
        CASE WHEN TRIM(a.ACTV_REASON_CODE) IN ('SOCBLD') THEN SUM(ACTV_AMT) ELSE 0 END AS CONTRACT_SOC_BUILD_AMT,
       -- CASE WHEN TRIM(a.ACTV_REASON_CODE) IN ('PPCHNG') THEN SUM(ACTV_AMT) ELSE 0 END AS PP_CHANGE_AMT,
        CASE WHEN TRIM(a.ACTV_REASON_CODE) IN ('RCADJ','ACSSC','PPCHNG') THEN SUM(ACTV_AMT) ELSE 0 END AS RATE_PLAN_CHANGE_AMT,
        CASE WHEN TRIM(a.ACTV_REASON_CODE) NOT IN ('EXCACR','ERRAPN','BASE','DISC','DISREL','SD0031','RAFRBC', 'FTRESC', 'ERROR','ACTCH',
                    'AIRA','OGWVA','OGWDD','SMRTPH','LD','OGWVLD','RRVA','CVDRMW','AIRARO','PSTB','HST','PSTM','PSTQ','TAXG','TAXRND','TAXSK',
                    'BYOD24','RCADJ3','ENTCR','P2KRBC','RBC24M','SOCBLD', 'PPCHNG', 'RCADJ','ACSSC')
            THEN SUM(ACTV_AMT) ELSE 0 END AS OTHER_CREDIT_AMT,
        SUM(ACTV_AMT) ACTV_AMT
from  ADJUSTMENT@pol1_app a LEFT OUTER JOIN ACTIVITY_RSN_TEXT@pol1_app b
on a.ACTV_CODE = b.ACTV_CODE
and a.ACTV_REASON_CODE = b.ACTV_REASON_CODE
where BAN IN (33824591, 34128154, 36238435, 36238478, 36780322, 36780335, 36780343, 36780351)
-- if this is NULL in the source meaning the bill has not even been generated . These future credits entered when they sign up.
and a.ACTV_BILL_SEQ_NO is not null
--and subscriber_no='2262247318'
group by subscriber_no, a.ACTV_CODE ,a.ACTV_REASON_CODE --, b.bill_fixed_text, b.bill_text
) group by SUBSCRIBER_NO 
),
BILL_SEQ_BY_SUB AS (
select SUBSCRIBER_NO , BAN, LAST_BILL_SEQ_NO, BILL_DATE
FROM CHINATEL_W_BILL_SEQ_NO
),
SUB_BILL_LST_MON AS (
-- Get all the credits for the subscriber since activation date.
select a.subscriber_no, NVL(SUM(CURR_CREDIT_AMT),0) AS TOTAL_BILL_CREDIT_AMT,NVL(SUM(TOTAL_BILLED_ADJUST),0) AS TOTAL_BILLED_ADJUST_AMT
--, MIN(SYS_CREATION_DATE) as FIRST_BILL_DATE, MAX(SYS_CREATION_DATE) as LAST_BILL_DATE 
from bill_subscriber@pol1_app a
INNER JOIN BILL_SEQ_BY_SUB b
on a.subscriber_no = b.subscriber_no
and a.BILL_SEQ_NO = b.LAST_BILL_SEQ_NO
and a.BAN = b.BAN
where a.BAN IN (33824591, 34128154, 36238435, 36238478, 36780322, 36780335, 36780343, 36780351)
-- bill month for the previous month. If this is for June bill , input July as the bill date.
-- *************** CHANGE HERE 
and TRUNC(SYS_CREATION_DATE,'MM') = TO_DATE('2023-06-01 00:00:00','YYYY-MM-DD HH24:MI:SS')
group by a.subscriber_no
),
ADJUST_CRDT_AMT_LST_MON AS (
select -- Get all adjustments to get the breakdown
SUBSCRIBER_NO, SUM(MISC_AMT) MISC_AMT, SUM(ADDON_CORRECTION_AMT) ADDON_CORRECTION_AMT,
               SUM(BILL_CREDIT_REVERSAL_AMT) as BILL_CREDIT_REVERSAL_AMT, 
               SUM(ADJ_REVERSAL_AMT) ADJ_REVERSAL_AMT, SUM(USG_CORRECTION_AMT) USG_CORRECTION_AMT,
               SUM(TAX_CREDIT_AMT) TAX_CREDIT_AMT, SUM(BILL_CREDIT_AMT) BILL_CREDIT_AMT, SUM(CONTRACT_SOC_BUILD_AMT) CONTRACT_SOC_BUILD_AMT,
               --SUM(PP_CHANGE_AMT) PP_CHANGE_AMT, 
               SUM(RATE_PLAN_CHANGE_AMT) RATE_PLAN_CHANGE_AMT, SUM(OTHER_CREDIT_AMT) OTHER_CREDIT_AMT, SUM(ACTV_AMT) TOTAL_ACTV_AMT
FROM (
-- RBC24M added on feb 26th. This is a new reason code.
  select a.subscriber_no, a.ACTV_CODE ,a.ACTV_REASON_CODE,
          CASE WHEN TRIM(a.ACTV_REASON_CODE) IN ('EXCACR','ERRAPN','BASE','DISC','DISREL','SD0031','RAFRBC') THEN SUM(ACTV_AMT) ELSE 0 END AS MISC_AMT,
          CASE WHEN TRIM(a.ACTV_REASON_CODE) IN ('FTRESC') THEN SUM(ACTV_AMT) ELSE 0 END AS ADDON_CORRECTION_AMT,
          CASE WHEN TRIM(a.ACTV_REASON_CODE) IN ('ERROR') THEN SUM(ACTV_AMT) ELSE 0 END AS BILL_CREDIT_REVERSAL_AMT,
          CASE WHEN TRIM(a.ACTV_REASON_CODE) IN ('ACTCH') THEN SUM(ACTV_AMT) ELSE 0 END AS ADJ_REVERSAL_AMT,
          CASE WHEN TRIM(a.ACTV_REASON_CODE) IN ('AIRA','OGWVA','OGWDD','SMRTPH','LD','OGWVLD','RRVA','CVDRMW','AIRARO') THEN SUM(ACTV_AMT) ELSE 0 END AS USG_CORRECTION_AMT,
          CASE WHEN TRIM(a.ACTV_REASON_CODE) IN ('PSTB','HST','PSTM','PSTQ','TAXG','TAXRND','TAXSK') THEN SUM(ACTV_AMT) ELSE 0 END AS TAX_CREDIT_AMT,
          CASE WHEN TRIM(a.ACTV_REASON_CODE) IN ('BYOD24','RCADJ3','ENTCR','P2KRBC','RBC24M') THEN SUM(ACTV_AMT) ELSE 0 END AS BILL_CREDIT_AMT,
          CASE WHEN TRIM(a.ACTV_REASON_CODE) IN ('SOCBLD') THEN SUM(ACTV_AMT) ELSE 0 END AS CONTRACT_SOC_BUILD_AMT,
         -- CASE WHEN TRIM(a.ACTV_REASON_CODE) IN ('PPCHNG') THEN SUM(ACTV_AMT) ELSE 0 END AS PP_CHANGE_AMT,
          CASE WHEN TRIM(a.ACTV_REASON_CODE) IN ('RCADJ','ACSSC','PPCHNG') THEN SUM(ACTV_AMT) ELSE 0 END AS RATE_PLAN_CHANGE_AMT,
          CASE WHEN TRIM(a.ACTV_REASON_CODE) NOT IN ('EXCACR','ERRAPN','BASE','DISC','DISREL','SD0031','RAFRBC', 'FTRESC', 'ERROR','ACTCH',
                      'AIRA','OGWVA','OGWDD','SMRTPH','LD','OGWVLD','RRVA','CVDRMW','AIRARO','PSTB','HST','PSTM','PSTQ','TAXG','TAXRND','TAXSK',
                      'BYOD24','RCADJ3','ENTCR','P2KRBC','RBC24M','SOCBLD', 'PPCHNG', 'RCADJ','ACSSC')
              THEN SUM(ACTV_AMT) ELSE 0 END AS OTHER_CREDIT_AMT,
          SUM(ACTV_AMT) ACTV_AMT
  from  ADJUSTMENT@pol1_app a
  INNER JOIN BILL_SEQ_BY_SUB c
  on a.subscriber_no = c.subscriber_no
     and a.BAN = c.BAN
  and a.ACTV_BILL_SEQ_NO = c.LAST_BILL_SEQ_NO
  LEFT OUTER JOIN ACTIVITY_RSN_TEXT@pol1_app b
  on TRIM(a.ACTV_CODE) = TRIM(b.ACTV_CODE)
  and TRIM(a.ACTV_REASON_CODE) = TRIM(b.ACTV_REASON_CODE)
  where a.BAN IN (33824591, 34128154, 36238435, 36238478, 36780322, 36780335, 36780343, 36780351)
  group by a.subscriber_no, a.ACTV_CODE ,a.ACTV_REASON_CODE 
) group by SUBSCRIBER_NO 
)
-- Finally join all subscriber promo records with the total credits the customer got since opening the account.
select SUB_NO, --SUBSCRIBER_NO, 
    INIT_ACTIVATION_DATE, BAN, SOC, SOC_EFFECTIVE_DATE, ORIGINAL_SOC, NEXT_ORIGINAL_SOC, NEXT_TWO_ORIG_SOC, 
    EXPIRATION_DATE, MODIFIED_EXP_DATE, NEXT_SOC, NEXT_SOC_EFF_DATE, NEXT_SOC_EXP_DATE, START_DATE, END_DATE, NEXT_TWO_SOC, NEXT_TWO_SOC_EFF_DATE,
    NEXT_TWO_SOC_EXP_DATE, GRACE_PERIOD_DATE, CAMPAIGN_NOTE, ELIGIBLE_IND, PROMO_EXP_ORDER, MONTHS_ELIGIBLE, PROJECTED_FULL_CREDIT_AMT, PROMO_AMT, 
    MONTHS_INTO_PROMO, MOD_MONTHS_INTO_PROMO, DAYS_INTO_PROMO, ELIGIBLE_CREDIT_TO_DATE, FULL_WINDOW_IND, GRACE_WINDOW_IND, TIME_TO_VACATION_SOC, 
    DAYS_OUT_OF_VACATION, RECALC_ELIGIBLE_IND, PROMO_ORDER, MASTER_ELIGIBILITY, LATEST_BAN, LATEST_BAN_ACTV_DATE, PREV_BAN, PREV_BAN_ACTV_DATE, 
    BAN_CHANGE_IND, MONTHS_IN_FIRST_BAN, DAYS_TO_BAN_CHANGE, 
    TOTAL_BILL_CREDIT_AMT, TOTAL_BILLED_ADJUST_AMT,  
    MISC_AMT, ADDON_CORRECTION_AMT, BILL_CREDIT_REVERSAL_AMT, ADJ_REVERSAL_AMT, USG_CORRECTION_AMT, TAX_CREDIT_AMT, BILL_CREDIT_AMT, CONTRACT_SOC_BUILD_AMT,
    RATE_PLAN_CHANGE_AMT, OTHER_CREDIT_AMT, TOTAL_ACTV_AMT, RECALC_SOC_IND, FINAL_ELIGIBILITY, FINAL_MONTHS_INTO_PROMO, FINAL_ELIGIBLE_CREDIT_TO_DATE, 
    CASE WHEN FINAL_ELIGIBILITY like 'ELIGIBLE%' 
    -- This date will be the month you are generating the bill for.
    -- *************** CHANGE HERE .. if its for June credits, have June month.
    and TO_DATE('2023-05-01 00:00:00','YYYY-MM-DD HH24:MI:SS')
            BETWEEN TRUNC(SOC_EFFECTIVE_DATE,'MM') and TRUNC(LEAST(ADD_MONTHS(SOC_EFFECTIVE_DATE,FINAL_MONTHS_INTO_PROMO),MODIFIED_EXP_DATE),'MM')
    THEN 'CREDIT ELIGIBLE FOR PREVIOUS MONTH'
    END AS PREVIOUS_MONTH_IND,
    CASE WHEN FINAL_ELIGIBILITY like 'ELIGIBLE%'
    -- This date will be the month you are generating the bill for.
    -- *************** CHANGE HERE .. if its for June credits, have June month.
    and TO_DATE('2023-05-01 00:00:00','YYYY-MM-DD HH24:MI:SS') 
            BETWEEN TRUNC(SOC_EFFECTIVE_DATE,'MM') and TRUNC(LEAST(ADD_MONTHS(SOC_EFFECTIVE_DATE,FINAL_MONTHS_INTO_PROMO),MODIFIED_EXP_DATE),'MM')
    THEN PROMO_AMT
    END AS PREVIOUS_CREDIT,
    PREV_TOTAL_BILL_CRDT_AMT, PREV_BILL_CREDIT_AMT, PREV_TOTAL_ACTV_AMT,PREV_BILL_CRDT_REV_AMT, PREV_RATE_PLAN_CHNG_AMT,PROVINCE
    -- This will calculate the eligibility for the previous month for credit.
 from (
select a.SUBSCRIBER_NO as SUB_NO, a.*, a.PROVINCE_CD as PROVINCE,b.*,c.*,d.TOTAL_BILL_CREDIT_AMT AS PREV_TOTAL_BILL_CRDT_AMT, 
        e.BILL_CREDIT_AMT as PREV_BILL_CREDIT_AMT, e.TOTAL_ACTV_AMT as PREV_TOTAL_ACTV_AMT,
        e.BILL_CREDIT_REVERSAL_AMT as PREV_BILL_CRDT_REV_AMT, e.RATE_PLAN_CHANGE_AMT as PREV_RATE_PLAN_CHNG_AMT,
        CASE WHEN (SOC <> ORIGINAL_SOC) OR (NEXT_SOC <> NEXT_ORIGINAL_SOC) or (NEXT_TWO_SOC <> NEXT_TWO_ORIG_SOC)
             THEN 'SOC REPLACED' 
        END AS RECALC_SOC_IND,
        CASE WHEN MASTER_ELIGIBILITY like 'ELIGIBLE%' and BAN_CHANGE_IND = 'Y' THEN 'ELIGIBLE-PARTIAL-BAN-CHANGE' 
            ELSE MASTER_ELIGIBILITY
        END AS FINAL_ELIGIBILITY,
        CASE 
            WHEN BAN_CHANGE_IND = 'Y' and (DAYS_TO_BAN_CHANGE BETWEEN 0 and 28)  
            THEN 0
            WHEN BAN_CHANGE_IND = 'Y' and MOD_MONTHS_INTO_PROMO > MONTHS_IN_FIRST_BAN -- NEED to check if we are giving away credit for partial BAN change.  
            THEN MONTHS_IN_FIRST_BAN
            ELSE MOD_MONTHS_INTO_PROMO
        END AS FINAL_MONTHS_INTO_PROMO,
        (CASE 
            WHEN BAN_CHANGE_IND = 'Y' and (DAYS_TO_BAN_CHANGE BETWEEN 0 and 28)  
            THEN 0
            WHEN BAN_CHANGE_IND = 'Y' and MOD_MONTHS_INTO_PROMO > MONTHS_IN_FIRST_BAN 
            THEN MONTHS_IN_FIRST_BAN
            ELSE MOD_MONTHS_INTO_PROMO
        END) * a.PROMO_AMT AS FINAL_ELIGIBLE_CREDIT_TO_DATE,PROVINCE_CD
        from MASTER_SUBSCRIBER_ELIGIBILITY  a
                LEFT OUTER JOIN 
                SUBSCRIBER_BILL b
                ON a.subscriber_no = b.subscriber_no
                LEFT OUTER JOIN 
                ADJUSTMENT_CREDIT_AMT c
                on a.subscriber_no = c.subscriber_no
                LEFT OUTER JOIN SUB_BILL_LST_MON d
                ON a.subscriber_no = d.subscriber_no
                LEFT OUTER JOIN ADJUST_CRDT_AMT_LST_MON e
                ON a.subscriber_no = e.subscriber_no
) a;



/*
select sub_no, count(1)
from CHINATEL_T_PROMO_SUMMARY
group by sub_no

SELECT * from CHINATEL_T_PROMO_SUMMARY
where campaign_note = 'March 2023 Promo';

where
-- BAN = 33824591
--and PREVIOUS_CREDIT>0
--and 
sub_no='2369700198' 

-- sub_no IN ('9023888008','5142646922','2369690828','7807082845','6479757300')*/