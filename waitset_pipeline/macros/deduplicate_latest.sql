{% macro deduplicate_latest(source, partition_by, order_by) %}
    SELECT *
    FROM (
        SELECT
            *,
            row_number() OVER (
                partition BY {{ partition_by }}
                ORDER BY {{order_by}} DESC
            ) AS _rn
        FROM {{ source }}
    ) AS deduped
    WHERE _rn = 1
{% endmacro %}