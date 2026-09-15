{# Rows whose combination of columns appears more than once. #}
{% test unique_combination(model, columns) %}
    select {{ columns | join(', ') }}, count(*) as occurrences
    from {{ model }}
    group by {{ columns | join(', ') }}
    having count(*) > 1
{% endtest %}


{# Rows where the SQL expression is false or NULL. #}
{% test expression_is_true(model, expression) %}
    select *
    from {{ model }}
    where not coalesce({{ expression }}, false)
{% endtest %}
