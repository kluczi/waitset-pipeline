WITH src AS (
    SELECT 
        p.id,
        p.name,
        p.owner_id,
        p.created_at,
        p.updated_at,
        pc.heard_from,
        pc.product_type
    FROM {{ref('stg_convex__projects')}} p 
    JOIN {{ref('stg_convex__project_context')}} pc ON pc.project_id=p.id
)

-- project najwzysza instancja
-- project contex metadata onboarding etc etc
-- waitlists waitlisty (moze byc duzo waitlist w jednym projekcie)
-- i pod kazda waitlista mozna miec page


SELECT DISTINCT 
    s.id,
    s.name,
    s.owner_id,
    s.created_at,
    s.updated_at,
    s.heard_from,
    s.product_type
FROM src s