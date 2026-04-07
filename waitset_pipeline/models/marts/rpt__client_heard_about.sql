WITH src AS (
    SELECT 
        COUNT(u.email) as user_cnt,
        p.heard_from
    FROM {{ref('dim_user')}} u 
    JOIN {{ref('dim_project')}} p ON u.id=p.owner_id
    GROUP BY 2
)

SELECT 
    s.heard_from,
    s.user_cnt
FROM src s

