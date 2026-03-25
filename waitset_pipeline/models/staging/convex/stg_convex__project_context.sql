WITH src AS (
    SELECT
        loaded_at::timestamp as loaded_at,
        (payload->>'_id')::text as id, 
        to_timestamp((payload->>'createdAt')::float/1000)::timestamp as created_at,
        (payload->>'heardAboutUs')::text as heard_from,
        payload->'productType'->>'value'::text as product_type,
        (payload->>'projectId')::text as project_id,
        to_timestamp((payload->>'updatedAt')::float/1000)::timestamp as updated_at
    FROM {{source('raw', 'raw_convex_project_context')}}
),
deduped AS (
    {{deduplicate_latest('src', 'id', 'loaded_at')}}
)

SELECT
    deduped.id,
    deduped.created_at,
    deduped.heard_from, 
    deduped.product_type,
    deduped.project_id,
    deduped.updated_at
FROM deduped