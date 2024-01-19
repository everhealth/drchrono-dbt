{{ config(
    materialized = 'view',
    sort = ['created_at', 'scheduled_time', 'practice_group_id', 'doctor_id'],
    auto_refresh = 'true'
) }}
{% set chronometer_schema = 'chronometer_production_chronometer_production' %}

SELECT
    bli.id ,
    bli.created_at ,
    bli.code ,
    bli.billed ,
    ca.patient_id ,
    cp.first_name ,
    cp.middle_name ,
    cp.last_name ,
    cp.chart_id ,
    co.name ,
    ca.office_id ,
    ca.doctor_id ,
    cd.firstName ,
    cd.lastName ,
    cd.salutation ,
    cd.suffix ,
    cd.practice_group_id ,
    bli.appointment_id ,
    ca.billing_provider_id ,
    ca.supervising_provider_id,
    ca.examination_room AS exam_room_id,
    {{ exam_room_name()}} ,
    ca.created_at AS appt_created_at ,
    ca.service_date_start_date ,
    ca.service_date_end_date ,
    ca.first_billed_date ,
    ca.last_billed_date ,
    ca.scheduled_time ,
    ca.institutional_claim_flag ,
    ca.appointment_status ,
    ca.reason
FROM  {{ source(chronometer_schema, 'billing_billinglineitem') }} bli
JOIN {{ source(chronometer_schema, 'chronometer_appointment') }} ca
    ON (bli.appointment_id = ca.id)
JOIN {{ source(chronometer_schema, 'chronometer_doctor') }} cd
    ON (ca.doctor_id = cd.id)
JOIN {{ source(chronometer_schema, 'chronometer_patient') }} cp
    ON (ca.patient_id = cp.id)
JOIN {{ source(chronometer_schema, 'chronometer_office') }} co
    ON (ca.office_id = co.id)
WHERE ( ca.deleted_flag IS FALSE
        AND ca.appt_is_break IS FALSE
        AND ca.is_demo_data_appointment IS FALSE
        AND cp.is_demo_data_patient IS FALSE
        AND NOT ( COALESCE(appointment_status, '') = 'No Show'
        AND bli.procedure_type IN ('C', 'H', 'R') )
        AND NOT ( COALESCE(appointment_status, '') IN ('Cancelled', 'Rescheduled') )
)
