{#
    Rows from the newest bronze snapshot of each series. Every snapshot holds a
    series' complete history, so older snapshots add nothing downstream.
#}
{% macro newest_snapshot(relation) %}
    select * exclude (snapshot_rank)
    from (
        select
            *,
            dense_rank() over (
                partition by series_id
                order by vintage_through desc, _ingested_at desc
            ) as snapshot_rank
        from {{ relation }}
    )
    where snapshot_rank = 1
{% endmacro %}
