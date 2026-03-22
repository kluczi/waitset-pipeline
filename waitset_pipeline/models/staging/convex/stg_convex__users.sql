WITH src AS (
    SELECT
        loaded_at::timestamp as loaded_at,
        (payload->>'_id')::text as id,
        to_timestamp((payload->>'createdAt')::float/1000)::timestamp as created_at,
        (payload->>'email')::text as email,
        (payload->>'name')::text as name
    FROM {{source('raw', 'raw_convex_users')}}
),

deduped AS (
    {{deduplicate_latest('src', 'id', 'loaded_at')}}
)

SELECT
    deduped.id,
    deduped.email,
    deduped.name,
    deduped.created_at
FROM deduped