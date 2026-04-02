WITH src AS (
    SELECT
        u.user_id as id,
        u.email as email,
        u.name as name,
        ust.last_activity_date as last_activity_date,
        ust.last_subscription_status as last_subscription_status,
        ust.subscription_start_date as subscription_start_date
    FROM {{ref('stg_convex__users')}} u
    JOIN {{ref('stg_convex__user_subscription_tracking')}} ust ON u.user_id=ust.user_id
)

SELECT DISTINCT
    src.id,
    src.email,
    src.name,
    src.last_subscription_status, 
    src.subscription_start_date,
    src.last_activity_date
FROM src