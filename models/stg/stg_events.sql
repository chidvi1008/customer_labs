{{ config(materialized='view') }}

select
    cast(event_timestamp as timestamp) as event_ts,
    event_name,
    session_id,
    user_pseudo_id,
    source,
    medium,
    campaign,
    page_location
from read_csv_auto('data/sample_ga4_events.csv')

