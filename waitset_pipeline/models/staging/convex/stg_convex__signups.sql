WITH src AS (
    SELECT
        loaded_at::timestamp as loaded_at,
        (payload->>'_id')::text as id,
        (payload->>'contact'):: text as contact_mail,
        to_timestamp((payload->>'createdAt')::float/1000)::timestamp as created_at,
        (payload->>'pageId')::text as page_id,
        (payload->>'waitlistId'):: text as waitlist_id
    FROM {{source('raw', 'raw_convex_signups')}}
),

deduped AS (
    {{deduplicate_latest('src', 'id', 'loaded_at')}}
)

SELECT
    deduped.id,
    deduped.contact_mail,
    deduped.created_at,
    deduped.page_id,
    deduped.waitlist_id
FROM deduped