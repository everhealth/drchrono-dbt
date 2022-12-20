{{ config(
    materialized = 'materialized_view',
    sort = ['doctor_id', 'posted_date', 'scheduled_time'],
    auto_refresh = 'true',
    tags=["DaySheet"]
) }}

SELECT
{{ daysheet_pp_columns(True, True)}}

FROM
{{ source('chronometer_production', 'billing_cashpayment') }} bcp
JOIN {{ source('chronometer_production', 'chronometer_appointment') }} ca
ON (
    bcp.appointment_id = ca.id
    AND ca.deleted_flag IS FALSE
    AND ca.is_demo_data_appointment IS FALSE
    AND ca.appt_is_break IS FALSE
    AND COALESCE(
        ca.appointment_status,
        ''
    ) NOT IN (
        'No Show',
        'Cancelled',
        'Rescheduled'
    )
)
JOIN {{ source('chronometer_production', 'chronometer_doctor') }} cd
    ON (ca.doctor_id = cd.id)
JOIN {{ source('chronometer_production', 'chronometer_patient') }} cp
    ON (bcp.patient_id = cp.id)
JOIN {{ source('chronometer_production', 'chronometer_office') }} co
    ON (ca.office_id = co.id)
JOIN {{ source('chronometer_production', 'billing_billinglineitem') }} bli
    ON (bcp.line_item_id = bli.id)
WHERE
    amount <> 0
