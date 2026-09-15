{{ config(location=lake_uri('gold/release_revisions.parquet')) }}

-- For every observation, its headline number as first released and as it
-- stands in the newest vintage. A change from the previous period always uses
-- that period's value as it was known on the same day, so a first release is
-- never compared with a later revision of its neighbor.

with versions as (
    select * from {{ ref('observation_versions') }}
),

series as (
    select
        measures.series_id,
        measures.measure,
        metadata.frequency
    from {{ ref('series_measures') }} as measures
    inner join {{ ref('stg_fred__series') }} as metadata using (series_id)
),

observations as (
    select
        versions.series_id,
        versions.observation_date,
        series.measure,
        cast(
            case series.frequency
                when 'W' then versions.observation_date - interval 7 day
                when 'M' then versions.observation_date - interval 1 month
                when 'Q' then versions.observation_date - interval 3 month
            end as date
        ) as prior_date,
        count(*) as versions,
        min(versions.valid_from) filter (where versions.value is not null) as first_release_date,
        max(versions.valid_from) filter (where versions.is_current) as latest_release_date
    from versions
    inner join series using (series_id)
    group by all
),

known as (
    select
        observations.*,
        first_version.value as first_value,
        first_prior.value as first_prior_value,
        latest_version.value as latest_value,
        latest_prior.value as latest_prior_value
    from observations
    left join versions as first_version
        on first_version.series_id = observations.series_id
        and first_version.observation_date = observations.observation_date
        and first_version.valid_from = observations.first_release_date
    left join versions as first_prior
        on first_prior.series_id = observations.series_id
        and first_prior.observation_date = observations.prior_date
        and observations.first_release_date between first_prior.valid_from and first_prior.valid_to
    left join versions as latest_version
        on latest_version.series_id = observations.series_id
        and latest_version.observation_date = observations.observation_date
        and latest_version.is_current
    left join versions as latest_prior
        on latest_prior.series_id = observations.series_id
        and latest_prior.observation_date = observations.prior_date
        and latest_prior.is_current
),

headlines as (
    select
        *,
        {{ headline('measure', 'first_value', 'first_prior_value') }} as first_headline,
        {{ headline('measure', 'latest_value', 'latest_prior_value') }} as latest_headline
    from known
)

select
    series_id,
    observation_date,
    measure,
    versions,
    first_release_date,
    first_value,
    first_headline,
    latest_release_date,
    latest_value,
    latest_headline,
    latest_headline - first_headline as revision
from headlines
order by series_id, observation_date
