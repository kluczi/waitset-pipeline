WITH src AS (
    SELECT
        w.id,
        w.name,
        w.is_public,
        w.project_id
    FROM {{ ref('stg_convex__waitlists') }} w
)

SELECT DISTINCT
    src.id,
    src.name,
    src.is_public,
    src.project_id
FROM src