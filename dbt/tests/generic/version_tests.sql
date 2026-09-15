{#
    Tests for version histories: rows keyed by `key`, each valid from
    `valid_from` through `valid_to` inclusive, the current one ending 9999-12-31.
#}

{# Versions whose successor starts anywhere but the next day: earlier is an overlap, later is a gap. #}
{% test versions_are_contiguous(model, key, valid_from='valid_from', valid_to='valid_to') %}
    select *
    from (
        select
            {{ key | join(', ') }},
            {{ valid_from }} as valid_from,
            {{ valid_to }} as valid_to,
            lead({{ valid_from }}) over (
                partition by {{ key | join(', ') }} order by {{ valid_from }}
            ) as next_valid_from
        from {{ model }}
    )
    where next_valid_from is not null
        and next_valid_from <> valid_to + 1
{% endtest %}


{# Keys without exactly one open-ended version. #}
{% test one_current_version(model, key, valid_to='valid_to') %}
    select
        {{ key | join(', ') }},
        count(*) filter (where {{ valid_to }} = date '9999-12-31') as current_versions
    from {{ model }}
    group by {{ key | join(', ') }}
    having count(*) filter (where {{ valid_to }} = date '9999-12-31') <> 1
{% endtest %}


{# Versions that repeat the value before them, which would not be a real revision. #}
{% test consecutive_versions_differ(model, key, value='value', valid_from='valid_from') %}
    select *
    from (
        select
            {{ key | join(', ') }},
            {{ valid_from }} as valid_from,
            {{ value }} as value,
            lag({{ value }}) over ordered as previous_value,
            row_number() over ordered as version_number
        from {{ model }}
        window ordered as (partition by {{ key | join(', ') }} order by {{ valid_from }})
    )
    where version_number > 1
        and value is not distinct from previous_value
{% endtest %}
