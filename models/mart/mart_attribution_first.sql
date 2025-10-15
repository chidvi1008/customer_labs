{{ config(materialized='table') }}

WITH ranked_events AS (
  SELECT
    user_pseudo_id,
    source,
    medium,
    campaign,
    event_ts,
    ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_ts ASC) AS rn
  FROM {{ ref('stg_events') }}
)

SELECT
  user_pseudo_id,
  source AS first_source,
  medium AS first_medium,
  campaign AS first_campaign,
  event_ts AS first_click_ts
FROM ranked_events
WHERE rn = 1;
