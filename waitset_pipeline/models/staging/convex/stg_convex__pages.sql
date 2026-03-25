WITH src AS (
    SELECT
        loaded_at::timestamp as loaded_at,
        (payload->>'_id')::text as id,
        to_timestamp((payload->>'createdAt')::float/1000)::timestamp as created_at,
        (payload->>'createdBy')::text as created_by,
        (payload->>'domain')::text as domain,
        (payload->>'enabled')::boolean as is_enabled,
        (payload->>'templateKey'):: text as template_key,
        to_timestamp((payload->>'updatedAt')::float/1000)::timestamp as updated_at,
        (payload->>'updatedBy')::text as updated_by,
        (payload->>'waitlistId'):: text as waitlist_id
    FROM {{source('raw', 'raw_convex_pages')}}
),

deduped as (
    {{deduplicate_latest('src', 'id', 'loaded_at')}}
)

SELECT 
    deduped.id,
    deduped.created_at,
    deduped.created_by, 
    deduped.domain, 
    deduped.is_enabled, 
    deduped.template_key,
    deduped.updated_at,
    deduped.updated_by,
    deduped.waitlist_id
FROM deduped