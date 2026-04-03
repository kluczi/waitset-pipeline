WITH helper AS (
    SELECT
        EXTRACT(DAY FROM (CURRENT_TIMESTAMP-u.last_activity_date)) AS days_since_last_activity, 
        u.id AS user_id
    FROM {{ref('dim_user')}} u 

),

rpt AS (
    SELECT
        COUNT(h.user_id) AS active_users
    FROM helper h
    WHERE h.days_since_last_activity < 31
)

SELECT
    active_users
FROM rpt