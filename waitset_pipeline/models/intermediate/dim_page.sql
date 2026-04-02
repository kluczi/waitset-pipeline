WITH src AS (
    SELECT
        p.id,
        p.is_enabled,
        p.template_key,
        p.waitlist_id
    FROM {{ref('stg_convex__pages')}} p 
)

SELECT DISTINCT
    s.id,
    s.is_enabled, 
    s.template_key,
    s.waitlist_id
FROM src s