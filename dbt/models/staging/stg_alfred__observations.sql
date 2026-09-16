-- The newest snapshot of each series. Every snapshot holds that series' full
-- history as of the moment it was taken, so older snapshots add nothing here;
-- they are kept for the closed-history test.

select
    series_id,
    observation_date,
    value,
    valid_from,
    valid_to,
    snapshot_vintage_through,
    snapshot_sha256
from {{ ref('stg_alfred__observation_snapshots') }}
qualify dense_rank() over (
    partition by series_id
    order by snapshot_vintage_through desc, snapshot_ingested_at desc
) = 1
