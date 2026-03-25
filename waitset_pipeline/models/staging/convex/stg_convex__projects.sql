WITH src AS (
    SELECT 
        loaded_at::timestamp as loaded_at, 
        (payload->>'_id')::text as id,
        to_timestamp((payload->>'createdAt')::float/1000)::timestamp as created_at, 
        (payload->>'createdBy'):: text as created_by,
        (payload->>'isDeleted')::boolean as is_deleted,
        (payload->>'name')::text as name,
        (payload->>'ownerId')::text as owner_id,
        to_timestamp((payload->>'updatedAt')::float/1000)::timestamp as updated_at, 
        (payload->>'updatedBy')::text as updated_by
    FROM {{source('raw', 'raw_convex_projects')}}
),

deduped AS (
    {{ deduplicate_latest('src', 'id', 'loaded_at') }}
)

SELECT 
    deduped.id,
    deduped.created_at,
    deduped.created_by,
    deduped.is_deleted, 
    deduped.name,
    deduped.owner_id,
    deduped.updated_at, 
    deduped.updated_by
FROM deduped