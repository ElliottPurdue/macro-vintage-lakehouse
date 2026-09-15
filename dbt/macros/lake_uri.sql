{# s3:// URI for a path inside the lake bucket. #}
{% macro lake_uri(path) -%}
    s3://{{ env_var('LAKE_S3_BUCKET') }}/{{ path }}
{%- endmacro %}
