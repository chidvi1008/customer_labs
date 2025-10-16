{{ config(materialized='table') }}

WITH deduped_events AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY event_timestamp, user_pseudo_id, event_name
            ORDER BY event_timestamp
        ) AS row_num
    FROM stg_events
)
,
session_agg AS (
    SELECT
        session_id,
        user_pseudo_id,
        (EXTRACT(EPOCH FROM MAX(event_timestamp) - MIN(event_timestamp)) / 60.0) AS session_duration_min,
        COUNT(*) AS events_in_session,
        CASE 
            WHEN (EXTRACT(EPOCH FROM MAX(event_timestamp) - MIN(event_timestamp)) / 60.0) > 60 
                THEN 1 
            ELSE 0 
        END AS is_long_session
    FROM deduped_events
    GROUP BY session_id, user_pseudo_id
    HAVING COUNT(*) > 1
)
,
first_last_click AS (
    SELECT
        user_pseudo_id,
        MIN(event_timestamp) AS first_click_ts,
        MAX(event_timestamp) AS last_click_ts,
        ARRAY_AGG(source ORDER BY event_timestamp)[1] AS first_source,
        ARRAY_AGG(medium ORDER BY event_timestamp)[1] AS first_medium,
        ARRAY_AGG(campaign ORDER BY event_timestamp)[1] AS first_campaign,
        ARRAY_AGG(source ORDER BY event_timestamp DESC)[1] AS last_source,
        ARRAY_AGG(medium ORDER BY event_timestamp DESC)[1] AS last_medium,
        ARRAY_AGG(campaign ORDER BY event_timestamp DESC)[1] AS last_campaign
    FROM deduped_events
    GROUP BY user_pseudo_id
)

SELECT
    s.*,
    f.first_click_ts,
    f.first_source,
    f.first_medium,
    f.first_campaign,
    f.last_click_ts,
    f.last_source,
    f.last_medium,
    f.last_campaign
FROM session_agg s
LEFT JOIN first_last_click f
ON s.user_pseudo_id = f.user_pseudo_id
