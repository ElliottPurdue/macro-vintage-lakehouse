{{ config(location=lake_uri('silver/observation_versions.parquet')) }}

select
    series_id,
    observation_date,
    row_number() over versions as version_number,
    value,
    lag(value) over versions as previous_value,
    valid_from,
    valid_to,
    valid_to = date '9999-12-31' as is_current,
    snapshot_vintage_through,
    snapshot_sha256
from {{ ref('stg_alfred__observations') }}
window versions as (partition by series_id, observation_date order by valid_from)
-- Sorted so each Parquet row group covers a narrow range of series and dates,
-- which lets readers skip row groups when filtering on them.
order by series_id, observation_date, valid_from
