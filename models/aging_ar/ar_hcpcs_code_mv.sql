{{ config(
    materialized = 'materialized_view',
    sort = ['doctor_id','code'],
    auto_refresh='true',
)}}

SELECT
    id,
    doctor_id,
    code,
    short_description
FROM {{source('chronometer_production','billing_hcpcslevel2code')}} hcpc
WHERE
    hcpc.archived = False
