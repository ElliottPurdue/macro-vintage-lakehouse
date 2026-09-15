-- A value can't be published before the period it describes has begun.
select series_id, observation_date, valid_from
from {{ ref('observation_versions') }}
where valid_from < observation_date
