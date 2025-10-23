{{ config(
    materialized = 'view',
    tags = ['staging']
) }}

SELECT
    CAST("geolocation_zip_code_prefix" AS INT) AS geolocation_zip_prefix,
    CAST("geolocation_lat" AS FLOAT) AS geolocation_lat,
    CAST("geolocation_lng" AS FLOAT) AS geolocation_lng,
    LOWER(TRIM("geolocation_city")) AS geolocation_city,
    LOWER(TRIM("geolocation_state")) AS geolocation_state
FROM {{ source('olist', 'olist_geolocation_dataset') }}
