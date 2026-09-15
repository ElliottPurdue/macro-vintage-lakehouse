-- Gold must keep exactly one row for every observation in silver.
with silver as (
    select count(*) as observations
    from (select distinct series_id, observation_date from {{ ref('observation_versions') }})
),

gold as (
    select count(*) as observations from {{ ref('release_revisions') }}
)

select silver.observations as silver_observations, gold.observations as gold_observations
from silver, gold
where silver.observations <> gold.observations
