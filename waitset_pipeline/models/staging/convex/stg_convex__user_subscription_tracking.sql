WITH src AS (
    SELECT
        loaded_at::timestamp as loaded_at,
        (payload ->> '_id')::varchar as id,
        to_timestamp((payload->>'lastActivityDate')::float/1000)::timestamp as last_activity_date,
        (payload->>'lastSubscriptionStatus')::varchar as last_subscription_status,
        to_timestamp((payload->>'subscriptionStartDate')::float/1000)::timestamp as subscription_start_date,
        (payload->>'userId')::varchar as user_id,
        to_timestamp((payload->>'_creationTime')::float/1000)::timestamp as creation_time
    FROM {{source('raw', 'raw_convex_user_subscription_tracking')}}
),

deduped AS (
    {{ deduplicate_latest('src','id','loaded_at') }}
)

SELECT 
    deduped.id,
    deduped.last_activity_date,
    deduped.last_subscription_status,
    deduped.subscription_start_date,
    deduped.user_id,
    deduped.creation_time
FROM deduped