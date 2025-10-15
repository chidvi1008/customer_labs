{{ config(materialized='table') }}

select
    session_id,
    user_pseudo_id,
    min(event_ts) as session_start,
    max(event_ts) as session_end
from {{ ref('stg_events') }}
group by 1,2
