{{ config(materialized='table') }}

select
    user_pseudo_id,
    any_value(source) as last_source,
    any_value(medium) as last_medium,
    any_value(campaign) as last_campaign,
    max(event_ts) as last_click_ts
from {{ ref('stg_events') }}
group by user_pseudo_id
