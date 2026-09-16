-- Every bronze snapshot, typed. The same observation appears once for each
-- snapshot that holds it, which is what lets older snapshots be compared with
-- the newest one.

select
    series_id,
    vintage_through as snapshot_vintage_through,
    -- A snapshot pulled as of a past date reports the periods that were current
    -- then as ending on that date. A full pull is open ended.
    cast(coalesce(_as_of, '9999-12-31') as date) as snapshot_as_of,
    _content_sha256 as snapshot_sha256,
    _ingested_at as snapshot_ingested_at,
    cast(date as date) as observation_date,
    -- ALFRED shows '.' for a period with no published value. Anything else
    -- that isn't a number should fail the cast rather than become NULL.
    case when value = '.' then null else cast(value as double) end as value,
    cast(realtime_start as date) as valid_from,
    cast(realtime_end as date) as valid_to
from {{ source('bronze', 'alfred_observations') }}
