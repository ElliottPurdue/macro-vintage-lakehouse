-- FRED lists a vintage date only when values were released or revised, so
-- each one must start at least one version.
select vintages.series_id, vintages.vintage_date
from {{ ref('stg_alfred__vintage_dates') }} as vintages
anti join {{ ref('observation_versions') }} as versions
    on versions.series_id = vintages.series_id
    and versions.valid_from = vintages.vintage_date
