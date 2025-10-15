{{ config(materialized='table') }}

SELECT
    'First Click' AS model_type,
    first_source AS source,
    COUNT(*) AS users
FROM {{ ref('int_events') }}
GROUP BY 1,2

UNION ALL

SELECT
    'Last Click' AS model_type,
    last_source AS source,
    COUNT(*) AS users
FROM {{ ref('int_events') }}
GROUP BY 1,2
