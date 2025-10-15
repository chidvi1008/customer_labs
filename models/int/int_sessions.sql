{{ config(materialized='table') }}

SELECT
    session_id,
    user_pseudo_id,
    MIN(event_ts) AS session_start,
    MAX(event_ts) AS session_end,
    TIMESTAMPDIFF(MINUTE, MIN(event_ts), MAX(event_ts)) AS session_duration_min,
    COUNT(*) AS events_in_session,
    ARRAY_AGG(event_name ORDER BY event_ts)[1] AS first_event,
    ARRAY_AGG(event_name ORDER BY event_ts DESC)[1] AS last_event,
    CASE 
        WHEN TIMESTAMPDIFF(MINUTE, MIN(event_ts), MAX(event_ts)) > 60 THEN 1
        ELSE 0
    END AS is_long_session
FROM {{ ref('stg_events') }}
GROUP BY 1,2
HAVING COUNT(*) > 1
ORDER BY session_start;

