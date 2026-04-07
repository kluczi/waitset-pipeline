WITH src AS (
    SELECT
        loaded_at::timestamp as loaded_at,
        (payload->>'id')::text as id,
        (payload->'properties'->>'email')::text as email,
        COALESCE((payload->'properties'->>'$geoip_country_name'), 'N/A')::text as country,
        COALESCE((payload->'properties'->>'$initial_geoip_city_name'), 'N/A') as city
    FROM {{source('raw', 'raw_posthog_persons')}} 
),

deduped AS (
    {{deduplicate_latest('src', 'id', 'loaded_at')}}
)

SELECT 
    s.email,
    s.city,
    s.country
FROM src s 