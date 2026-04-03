WITH rpt AS (
    SELECT
        w.id AS waitlist_id,
        p.name AS project_name,
        u.email AS owner_email,
        u.last_subscription_status AS subscription,
        COUNT(s.signup_id) AS signups_cnt
    FROM 
        {{ref('dim_project')}} p
        JOIN {{ref('dim_waitlist')}} w ON p.id=w.project_id 
        JOIN {{ref('fct_signup')}} s ON w.id=s.waitlist_id
        JOIN {{ref('dim_user')}} u ON u.id=p.owner_id
    GROUP BY 
        w.id, p.name, u.email, u.last_subscription_status
)

SELECT
    rpt.waitlist_id,
    rpt.project_name,
    rpt.owner_email,
    rpt.subscription,
    rpt.signups_cnt
FROM rpt