with snapshot as (
    {{ newest_snapshot(source('bronze', 'fred_series')) }}
)

select
    series_id,
    title,
    frequency_short as frequency,
    units,
    seasonal_adjustment_short as seasonal_adjustment,
    cast(observation_start as date) as observation_start,
    cast(observation_end as date) as observation_end
from snapshot
