WITH src AS (
    SELECT
        COUNT(p.id) AS active_pages
    FROM {{ref('dim_page')}} p 
    WHERE p.is_enabled=true
)

SELECT 
    s.active_pages
FROM src s