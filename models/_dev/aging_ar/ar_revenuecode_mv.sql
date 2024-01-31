{{ config(
    materialized = 'view',
    sort = ['code'],
)}}
SELECT
    id,
    code,
    description
FROM {{source('chronometer_production','billing_revenuecode')}} brc
