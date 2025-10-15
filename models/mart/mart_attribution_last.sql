{{ config(materialized='table') }}

select
    user_pseudo_id,
    any_value(source ignore nulls order by event_ts desc) as last_source,
    any_value(medium ignore nulls order by event_ts desc) as last_medium,
    any_value(campaign ignore nulls order by event_ts desc) as last_campaign,
    max(event_ts) as last_click_ts
from {{ ref('stg_events') }}
group by user_pseudo_id