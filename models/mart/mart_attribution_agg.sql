{{ config(materialized='table') }}

WITH attribution_union AS (
    SELECT 
        'First Click' AS model_type,
        first_source AS source,
        first_medium AS medium,
        CAST(first_click_ts AS DATE) AS event_date
    FROM {{ ref('int_events') }}

    UNION ALL

    SELECT 
        'Last Click' AS model_type,
        last_source AS source,
        last_medium AS medium,
        CAST(last_click_ts AS DATE) AS event_date
    FROM {{ ref('int_events') }}
)

SELECT
    model_type,
    source,
    medium,
    event_date,
    COUNT(*) AS user_count
FROM attribution_union
WHERE event_date >= current_date - INTERVAL 14 DAY
GROUP BY 1,2,3,4
ORDER BY event_date, model_type, source
