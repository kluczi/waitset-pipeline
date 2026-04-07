WITH src AS (
    SELECT 
        COUNT(p.id) AS product_cnt,
        p.product_type
    FROM {{ref('dim_project')}} p
    GROUP BY 2
)

SELECT
    s.product_type,
    s.product_cnt
FROM src s