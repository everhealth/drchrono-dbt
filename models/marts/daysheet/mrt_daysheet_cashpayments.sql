{{ config(
    sort = ['doctor_id', 'posted_date', 'scheduled_time']
) }}


SELECT
    -- CashPayment
    bcp.*,
    -- Patient
    cp.first_name AS patient_first_name,
    cp.middle_name AS patient_middle_name,
    cp.last_name AS patient_last_name,
    cp.chart_id AS patient_chart_id,
    co.name AS office_name,
    -- Doctor
    cd.firstname AS doctor_first_name,
    cd.lastname AS doctor_last_name,
    cd.salutation AS doctor_salutation,
    cd.suffix AS doctor_suffix,
    cd.practice_group_id,
    -- Appointment
    -- exam_room: ID and NAME
    ca.examination_room AS exam_room_id,
    {{ exam_room_name() }},
    ca.service_date_start_date,
    ca.service_date_end_date,  
    ca.first_billed_date,
    ca.scheduled_time,
    ca.institutional_claim_flag,
    bli.code as billing_code
FROM
    {{ ref('int_cashpayments') }} bcp
    LEFT JOIN {{source( 'chronometer_scrubbed', 'chronometer_appointment' ) }} ca
    ON (
        bcp.appointment_id = ca.id
        AND ca.deleted_flag IS FALSE
        AND ca.is_demo_data_appointment IS FALSE
        AND ca.appt_is_break IS FALSE
        AND COALESCE(ca.appointment_status,'') NOT IN ('Cancelled', 'Rescheduled')
    )
    JOIN {{ source( 'chronometer_scrubbed','chronometer_doctor') }} cd
    ON (
        ca.doctor_id = cd.id
    )
    JOIN {{source( 'chronometer_scrubbed', 'chronometer_patient' ) }} cp
    ON (
        bcp.patient_id = cp.id
    )
    JOIN {{source( 'chronometer_scrubbed', 'chronometer_office') }} co
    ON (
        ca.office_id = co.id
    )
    LEFT JOIN {{source( 'chronometer_scrubbed', 'billing_billinglineitem' ) }} bli
    ON (
        bcp.line_item_id = bli.id
    )
WHERE
    amount <> 0
