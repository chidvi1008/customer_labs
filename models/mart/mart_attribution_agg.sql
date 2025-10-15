{{ config(materialized='table') }}

select
    'First Click' as model_type,
    first_source as source,
    count(*) as users
from {{ ref('mart_attribution_first') }}
group by 1,2

union all

select
    'Last Click' as model_type,
    last_source as source,
    count(*) as users
from {{ ref('mart_attribution_last') }}
group by 1,2
