-- CustomProcedureCode
{{ config(
    materialized = 'materialized_view',
    sort = ['doctor_id', 'code'],
    auto_refresh='true',
)}}

SELECT
    id,
    doctor_id,
    code,
    "description"
FROM {{source('chronometer_production','chronometer_customprocedurecode')}} cpc
WHERE cpc.archived = False
