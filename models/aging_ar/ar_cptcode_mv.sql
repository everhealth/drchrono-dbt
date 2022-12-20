{{ config(
    materialized = 'materialized_view',
    sort = ['doctor_id', 'code'],
    auto_refresh='true',
)}}

SELECT
    id,
    doctor_id,
    code,
    medium_description
FROM {{source('chronometer_production','billing_cptcode')}} cbt
WHERE
    cpt.archived = False
