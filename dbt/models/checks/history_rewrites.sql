-- Places where the source changed its own past. ALFRED should never alter a
-- value it has already superseded, nor retract one it was still publishing, so
-- every row here is a problem and the model is expected to stay empty.
--
-- Each older snapshot is compared against the newest one. A snapshot taken as of
-- a past date reports the versions that were current then as ending on that
-- date, which is why the two cases below are checked differently.

with ranked as (
    select
        *,
        dense_rank() over (
            partition by series_id
            order by snapshot_vintage_through desc, snapshot_ingested_at desc
        ) as snapshot_rank
    from {{ ref('stg_alfred__observation_snapshots') }}
),

newest as (select * from ranked where snapshot_rank = 1),

older as (
    select
        *,
        -- The last date an older snapshot can speak for. One taken as of a past
        -- date speaks for that date. A full pull is open ended but has only seen
        -- releases up to its own newest one, and a version it showed as current
        -- being superseded by a later release is the source working normally.
        case
            when snapshot_as_of = date '9999-12-31' then snapshot_vintage_through
            else snapshot_as_of
        end as seen_through
    from ranked
    where snapshot_rank > 1
)

select
    older.series_id,
    older.snapshot_as_of,
    older.observation_date,
    older.valid_from,
    older.value as value_then,
    newest.value as value_now,
    older.valid_to as valid_to_then,
    newest.valid_to as valid_to_now,
    case
        when newest.valid_from is null then 'version disappeared'
        when newest.value is distinct from older.value then 'value changed after the fact'
        when older.valid_to < older.seen_through then 'a superseded version now ends elsewhere'
        else 'a version current then is now shown as having ended before the snapshot'
    end as problem
from older
left join newest
    on newest.series_id = older.series_id
    and newest.observation_date = older.observation_date
    and newest.valid_from = older.valid_from
where
    newest.valid_from is null
    or newest.value is distinct from older.value
    or (older.valid_to < older.seen_through and newest.valid_to <> older.valid_to)
    or (older.valid_to >= older.seen_through and newest.valid_to < older.seen_through)
