{#
    The headline number for an observation: the value itself, its change from
    the previous period, or its percent change, depending on the series' measure.
#}
{% macro headline(measure, value, prior_value) -%}
    case {{ measure }}
        when 'value' then {{ value }}
        when 'change' then {{ value }} - {{ prior_value }}
        when 'percent_change' then 100 * ({{ value }} / nullif({{ prior_value }}, 0) - 1)
    end
{%- endmacro %}
