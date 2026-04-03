WITH rpt AS (
    SELECT
        DATE_TRUNC('week', s.signup_at) AS week,
        COUNT(*) AS signups
    FROM {{ ref('fct_signup') }} s
    GROUP BY 1
    ORDER BY 1
)

SELECT
    rpt.week, 
    rpt.signups
FROM rpt
