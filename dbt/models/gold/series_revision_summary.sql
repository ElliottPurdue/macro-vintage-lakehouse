{{ config(location=lake_uri('gold/series_revision_summary.parquet')) }}

-- Revision statistics per series, over observations first released at least
-- var('mature_after_years') years before the series' newest vintage.

with revisions as (
    select * from {{ ref('release_revisions') }}
),

newest_vintage as (
    select series_id, max(vintage_date) as newest_vintage_date
    from {{ ref('stg_alfred__vintage_dates') }}
    group by series_id
),

mature as (
    select revisions.*
    from revisions
    inner join newest_vintage using (series_id)
    where revisions.first_headline is not null
        and revisions.latest_headline is not null
        and revisions.first_release_date
            <= newest_vintage.newest_vintage_date - interval {{ var('mature_after_years') }} year
)

select
    mature.series_id,
    measures.label,
    mature.measure,
    measures.revision_units,
    count(*) as observations,
    min(mature.first_release_date) as earliest_first_release,
    max(mature.first_release_date) as latest_first_release,
    -- Values arrive as decimal strings, so anything under 1e-9 is float noise.
    avg(case when abs(mature.revision) > 1e-9 then 1.0 else 0.0 end) as share_revised,
    -- The first estimate and the current value point in opposite directions.
    avg(case when mature.first_headline * mature.latest_headline < 0 then 1.0 else 0.0 end) as share_sign_flipped,
    avg(abs(mature.revision)) as mean_abs_revision,
    median(abs(mature.revision)) as median_abs_revision,
    avg(mature.revision) as mean_revision,
    arg_max(mature.observation_date, abs(mature.revision)) as largest_revision_date,
    arg_max(mature.revision, abs(mature.revision)) as largest_revision,
    avg(mature.versions) as mean_versions
from mature
inner join {{ ref('series_measures') }} as measures using (series_id)
group by all
order by mature.series_id
