WITH src AS (
    SELECT
        u.user_id as id,
        u.email as email,
        u.name as name,
        p.country as user_country,
        p.city as user_city,
        ust.last_activity_date as last_activity_date,
        ust.last_subscription_status as last_subscription_status,
        ust.subscription_start_date as subscription_start_date
    FROM {{ref('stg_convex__users')}} u
    JOIN {{ref('stg_convex__user_subscription_tracking')}} ust ON u.user_id=ust.user_id
    LEFT JOIN {{ref('stg_posthog__persons')}} p ON u.email=p.email
)

SELECT DISTINCT
    src.id,
    src.email,
    src.name,
    src.user_country,
    src.user_city,
    src.last_subscription_status, 
    src.subscription_start_date,
    src.last_activity_date
FROM src