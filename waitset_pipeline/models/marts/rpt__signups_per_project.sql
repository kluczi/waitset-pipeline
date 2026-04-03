WITH rpt AS (
    SELECT
        p.id AS project_id,
        COUNT(s.signup_id) AS signups_cnt
    FROM {{ ref('dim_project') }} p
    LEFT JOIN {{ ref('dim_waitlist') }} w
        ON w.project_id = p.id
    LEFT JOIN {{ ref('fct_signup') }} s
        ON s.waitlist_id = w.id
    GROUP BY p.id
)

SELECT
    AVG(signups_cnt) AS avg_signups_per_project
FROM rpt