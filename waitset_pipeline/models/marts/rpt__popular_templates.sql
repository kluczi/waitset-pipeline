WITH src AS (
    SELECT
        COUNT(p.id) AS templates_usage,
        p.template_key AS template_key
    FROM {{ref('dim_page')}} p
    GROUP BY 2
)

SELECT 
    s.template_key,
    s.templates_usage
FROM src s