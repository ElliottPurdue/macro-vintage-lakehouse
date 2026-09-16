{# Rows whose combination of columns appears more than once. #}
{% test unique_combination(model, columns) %}
    select {{ columns | join(', ') }}, count(*) as occurrences
    from {{ model }}
    group by {{ columns | join(', ') }}
    having count(*) > 1
{% endtest %}


{# For models that exist to collect problems: any row at all is a failure. #}
{% test is_empty(model) %}
    select * from {{ model }}
{% endtest %}


{# Rows where the SQL expression is false or NULL. #}
{% test expression_is_true(model, expression) %}
    select *
    from {{ model }}
    where not coalesce({{ expression }}, false)
{% endtest %}
