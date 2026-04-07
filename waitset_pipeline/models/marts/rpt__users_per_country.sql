WITH src AS (
    SELECT
        COUNT(u.id) AS user_cnt,
        u.user_country AS country
    FROM {{ref('dim_user')}} u
    GROUP BY 2
)

SELECT 
    src.country,
    src.user_cnt
FROM src