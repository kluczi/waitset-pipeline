WITH rpt AS (
    SELECT
    COUNT(s.signup_id) AS total_signups
    FROM {{ref('fct_signup')}} s
)

SELECT
    rpt.total_signups
FROM rpt