{{ config(
    materialized = 'materialized_view',
    sort = ['patient_id'],
    auto_refresh='true',
)}}

SELECT
    cbn.id,
    patient_id,
    "text",
    cbn.created_at,
    ca.institutional_claim_flag
FROM {{source('chronometer_production','billing_claimbillingnotes')}} cbn
JOIN {{source('chronometer_production','chronometer_appointment')}} ca
	ON (cbn.appointment_id = ca.id)
