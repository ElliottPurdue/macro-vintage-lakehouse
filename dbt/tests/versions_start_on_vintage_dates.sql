-- Every version must begin on one of its series' vintage dates.
select versions.series_id, versions.observation_date, versions.valid_from
from {{ ref('observation_versions') }} as versions
anti join {{ ref('stg_alfred__vintage_dates') }} as vintages
    on vintages.series_id = versions.series_id
    and vintages.vintage_date = versions.valid_from
