with snapshot as (
    {{ newest_snapshot(source('bronze', 'alfred_vintage_dates')) }}
)

select
    series_id,
    cast(vintage_date as date) as vintage_date
from snapshot
