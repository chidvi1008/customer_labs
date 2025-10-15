{{ config(materialized='view') }}

WITH raw_events AS (
    SELECT *
    FROM read_csv_auto('data/sample_ga4_events.csv')
),

cleaned_events AS (
    SELECT
        CAST(event_timestamp AS TIMESTAMP) AS event_ts,
        CAST(event_timestamp AS DATE) AS event_date,
        EXTRACT(HOUR FROM CAST(event_timestamp AS TIMESTAMP)) AS event_hour,
        event_name,
        session_id,
        user_pseudo_id,
        LOWER(TRIM(source)) AS source,
        LOWER(TRIM(medium)) AS medium,
        LOWER(TRIM(campaign)) AS campaign,
        REGEXP_REPLACE(page_location, 'https?://[^/]+', '') AS page_path
    FROM raw_events
    WHERE event_timestamp IS NOT NULL
      AND user_pseudo_id IS NOT NULL
      AND event_name IS NOT NULL
      AND source NOT IN ('', 'unknown')
      -- Optional: remove bot/internal traffic
      AND user_pseudo_id NOT LIKE 'internal_%'
),

deduplicated_events AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY event_ts, user_pseudo_id, event_name
                   ORDER BY event_ts
               ) AS row_num
        FROM cleaned_events
    ) t
    WHERE row_num = 1
)

SELECT *
FROM deduplicated_events
ORDER BY event_ts;