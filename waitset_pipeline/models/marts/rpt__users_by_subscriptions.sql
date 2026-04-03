WITH rpt AS ( 
    SELECT
        u.last_subscription_status AS subscription,
        COUNT(u.id) AS users_cnt
    FROM {{ref('dim_user')}} u
    GROUP BY u.last_subscription_status
)

SELECT
    rpt.subscription,
    rpt.users_cnt
FROM rpt