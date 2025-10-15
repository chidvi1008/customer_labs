{{ config(materialized='table') }}

select
    user_pseudo_id,
    any_value(source) as first_source,
    any_value(medium) as first_medium,
    any_value(campaign) as first_campaign,
    min(event_ts) as first_click_ts
from {{ ref('stg_events') }}
group by user_pseudo_id

