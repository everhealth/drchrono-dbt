{{ config(
    materialized = 'view',
    sort = ['doctor_id', 'posted_date', 'scheduled_time'],
    auto_refresh = 'true'
) }}

SELECT
{{ daysheet_pp_columns(False, False)}}

FROM
{{ source('chronometer_production', 'billing_cashpayment') }} bcp
JOIN {{ source('chronometer_production', 'chronometer_doctor') }} cd
    ON (ca.doctor_id = cd.id)
JOIN {{ source('chronometer_production', 'chronometer_patient') }} cp
    ON (bcp.patient_id = cp.id)
JOIN {{ source('chronometer_production', 'chronometer_office') }} co
    ON (ca.office_id = co.id)
WHERE
    amount <> 0
    AND bcp.line_item_id IS NULL
    AND bcp.appointment_id IS NULL
