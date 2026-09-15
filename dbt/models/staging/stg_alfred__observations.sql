with snapshot as (
    {{ newest_snapshot(source('bronze', 'alfred_observations')) }}
)

select
    series_id,
    cast(date as date) as observation_date,
    -- ALFRED shows '.' for a period with no published value. Anything else
    -- that isn't a number should fail the cast rather than become NULL.
    case when value = '.' then null else cast(value as double) end as value,
    cast(realtime_start as date) as valid_from,
    cast(realtime_end as date) as valid_to,
    vintage_through as snapshot_vintage_through,
    _content_sha256 as snapshot_sha256
from snapshot
