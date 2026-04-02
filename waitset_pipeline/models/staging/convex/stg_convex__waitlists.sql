WITH src AS (
    SELECT 
        loaded_at::timestamp AS loaded_at,
        (payload->>'_id')::text AS id,
        to_timestamp((payload->>'createdAt')::float/1000)::timestamp AS created_at,
        (payload->>'createdBy')::text AS created_by,
        (payload->>'isPublic')::boolean AS is_public,
        (payload->>'name')::text AS name,
        (payload->>'projectId')::text AS project_id, 
        to_timestamp((payload->>'updatedAt')::float/1000)::timestamp AS updated_at,
        (payload->>'updatedBy')::text AS updated_by
    FROM {{source('raw', 'raw_convex_waitlists')}}
),

deduped AS (
    {{deduplicate_latest('src', 'id', 'loaded_at')}}
)

SELECT 
    d.id,
    d.created_at, 
    d.created_by,
    d.is_public,
    d.name,
    d.project_id,
    d.updated_at,
    d.updated_by
FROM deduped d