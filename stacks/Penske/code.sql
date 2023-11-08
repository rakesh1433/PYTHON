Pagelevel Data

CREATE OR REPLACE TABLE
  `pmc-analytical-data-mart.Quantiphi_analysis.intermediate_page_view_complete` AS
SELECT
  DATE(time) AS event_dt,
  CONCAT("Week ", EXTRACT(WEEK FROM DATE(time))) AS week_number,
  SUBSTRING(FORMAT_DATE("%B", DATE(time)),0, 3) AS month_name,
  event_id,
  session_id AS session_id_pageviewComplete,
  view_id,
  properties.geo_info.continent,
  properties.geo_info.country,
  properties.geo_info.city,
  properties.geo_info.province,
  properties.client.domain,
  properties.client.url,
  CASE
        WHEN properties.client.referrer LIKE '%google%' THEN 'Google'
        WHEN properties.client.referrer LIKE '%facebook%' THEN 'Facebook'
        WHEN properties.client.referrer LIKE '%reddit%' THEN 'Reddit'
        WHEN properties.client.referrer LIKE '%amazon%' THEN 'Amazon'
        WHEN properties.client.referrer LIKE '%youtube%' THEN 'Youtube'
        WHEN properties.client.referrer LIKE '%instagram%' THEN 'Instagram'
        WHEN properties.client.referrer LIKE '%snapchat%' THEN 'Snapchat'
        WHEN properties.client.referrer LIKE '%twitter%' THEN 'Twitter'
        WHEN properties.client.referrer LIKE '%tiktok%' THEN 'Tik Tok'
        WHEN properties.client.referrer LIKE '%bing%' THEN 'Bing'
        ELSE 'Other'
        END AS referrer,
  properties.aggregations.PageviewEngagement.completion AS time_completion,
  properties.aggregations.PageviewEngagement.engaged_time AS engaged_time
FROM
  `pmc-analytical-data-mart.permutive.PageviewComplete_events` a
WHERE
  DATE(a._PARTITIONTIME) >= DATE_SUB(CURRENT_DATE()-1, INTERVAL 1 DAY)
  AND properties.article.watson.entities[SAFE_OFFSET(0)].relevance >0.6
  AND properties.article.watson.keywords [SAFE_OFFSET(0)].relevance >0.6
  AND properties.article.watson.concepts[SAFE_OFFSET(0)].relevance > 0.6
  AND properties.geo_info.country = "United States"

Segments/Tags

CREATE OR REPLACE TABLE
`pmc-analytical-data-mart.Quantiphi_analysis.intermediate_page_view_complete_segment`AS
WITH
  pageview AS(
  SELECT
    DISTINCT a.event_id,
    DATE(time) AS event_dt,
    segment
  FROM
    `pmc-analytical-data-mart.permutive.PageviewComplete_events`a,
    UNNEST(segments)AS segment
  WHERE
    DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE()-1, INTERVAL 1 DAY)),
  metadata AS (
  SELECT
    DISTINCT REPLACE(ARRAY_REVERSE(SPLIT(b.name,'-'))[SAFE_OFFSET(0)], '�','' ) AS name,
    b.number
  FROM
    `pmc-analytical-data-mart.Quantiphi_analysis.intermediate_segment_metadata` b
  WHERE
    b.tag LIKE "%Reporting%")
SELECT
  DISTINCT pageview.event_dt,
  pageview.event_id,
  STRING_AGG(DISTINCT metadata.name
  ORDER BY
    metadata.name) AS metadata_name,
FROM
  pageview
JOIN
  metadata
ON
  pageview.segment = metadata.number
GROUP BY
  1,
  2

Final

CREATE OR REPLACE TABLE
  `pmc-analytical-data-mart.Quantiphi_analysis.page_view_complete_dashboard` AS
SELECT
  pvCompleteData.*,
  pvCompleteSegmentData.metadata_name, 
  domainsData.Filter,
  advertiserData.Advertiser_name
FROM
  `pmc-analytical-data-mart.Quantiphi_analysis.intermediate_page_view_complete` AS pvCompleteData
JOIN  `pmc-analytical-data-mart.Quantiphi_analysis.intermediate_page_view_complete_segment` AS pvCompleteSegmentData
USING
  (event_id)
LEFT JOIN
  `pmc-analytical-data-mart.Quantiphi_analysis.top_domains` AS domainsData
ON
  pvCompleteData.domain = domainsData.Domains
LEFT JOIN
  `pmc-analytical-data-mart.Quantiphi_analysis.prebid_advertiser_name`AS advertiserData
USING
  (view_id)
















Behaviours  




CREATE OR REPLACE TABLE
  `pmc-analytical-data-mart.Quantiphi_analysis.intermediate_slot_clicked` AS
SELECT DISTINCT * FROM (
SELECT
  DATE(time) AS event_dt,
  CONCAT("Week ", EXTRACT(WEEK
    FROM
      DATE(time))) AS week_number,
  SUBSTRING(FORMAT_DATE("%B", DATE(time)),0, 3) AS month_name,
  event_id,
  session_id,
  view_id,
  properties.client.domain,
  properties.client.url,
  CASE
    WHEN properties.client.referrer LIKE '%google%' THEN 'Google'
    WHEN properties.client.referrer LIKE '%facebook%' THEN 'Facebook'
    WHEN properties.client.referrer LIKE '%reddit%' THEN 'Reddit'
    WHEN properties.client.referrer LIKE '%amazon%' THEN 'Amazon'
    WHEN properties.client.referrer LIKE '%youtube%' THEN 'Youtube'
    WHEN properties.client.referrer LIKE '%instagram%' THEN 'Instagram'
    WHEN properties.client.referrer LIKE '%snapchat%' THEN 'Snapchat'
    WHEN properties.client.referrer LIKE '%twitter%' THEN 'Twitter'
    WHEN properties.client.referrer LIKE '%tiktok%' THEN 'Tik Tok'
    WHEN properties.client.referrer LIKE '%bing%' THEN 'Bing'
  ELSE
  'Other'
END
  AS referrer,
  CONCAT(safe_cast (properties.height AS string),"x", SAFE_CAST(properties.width AS string) ) AS creative_size,
FROM
  `pmc-analytical-data-mart.permutive.SlotClicked_events`
  WHERE
  DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE()-1, INTERVAL 1 DAY)
  ) as slotClickedData
  JOIN
  (
SELECT
  session_id,
  properties.geo_info.continent,
  properties.geo_info.country,
  properties.geo_info.city,
  properties.geo_info.province
FROM
  `pmc-analytical-data-mart.permutive.Pageview_events`
WHERE
  DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE()-1, INTERVAL 30 DAY)
  AND properties.article.watson.entities[SAFE_OFFSET(0)].relevance >0.6
  AND properties.article.watson.keywords [SAFE_OFFSET(0)].relevance >0.6
  AND properties.article.watson.concepts[SAFE_OFFSET(0)].relevance>0.6
  AND properties.geo_info.country = "United States"
  ) AS pageviewData
  USING(session_id)


Tags data 

CREATE OR REPLACE TABLE
  `pmc-analytical-data-mart.Quantiphi_analysis.intermediate_slot_clicked_segment`AS
WITH
  pageview AS(
  SELECT
    DISTINCT 
    a.event_id,
    DATE(time) AS event_dt,
    segment
  FROM
    `pmc-analytical-data-mart.permutive.SlotClicked_events` a,
    UNNEST(segments)AS segment
  WHERE
    DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE()-1, INTERVAL 1 DAY)),
  metadata AS (
  SELECT
    DISTINCT
    REPLACE(ARRAY_REVERSE(SPLIT(b.name,'-'))[SAFE_OFFSET(0)], '�','' ) as name,
    b.number
  FROM
    `pmc-analytical-data-mart.Quantiphi_analysis.intermediate_segment_metadata` b
  WHERE
    b.tag LIKE "%Reporting%")
SELECT DISTINCT
  pageview.event_dt,
  pageview.event_id,
  metadata.name
FROM
  pageview
JOIN
  metadata
ON
  pageview.segment = metadata.number



Final

CREATE OR REPLACE TABLE
  `pmc-analytical-data-mart.Quantiphi_analysis.slot_clicked_dashboard` AS
SELECT
  pvCompleteData.*,
  pvCompleteSegmentData.name as metadata_name,
  domainsData.Filter,
  advertiserData.Advertiser_name
FROM
  `pmc-analytical-data-mart.Quantiphi_analysis.intermediate_slot_clicked` AS pvCompleteData
JOIN
  `pmc-analytical-data-mart.Quantiphi_analysis.intermediate_slot_clicked_segment` AS pvCompleteSegmentData
USING
  (event_id)
LEFT JOIN
  `pmc-analytical-data-mart.Quantiphi_analysis.top_domains` AS domainsData
ON
  pvCompleteData.domain = domainsData.Domains
LEFT JOIN
  `pmc-analytical-data-mart.Quantiphi_analysis.prebid_advertiser_name`
 AS advertiserData
USING
  (view_id)





