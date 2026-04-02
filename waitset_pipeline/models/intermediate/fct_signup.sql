WITH src AS (
    SELECT
        s.id as signup_id, 
        s.created_at as signup_at,
        s.waitlist_id,
        s.page_id
    FROM {{ref('dim_waitlist')}} w 
)

SELECT 
    src.signup_id,
    src.signup_at,
    src.waitlist_id,
    src.page_id
FROM src 