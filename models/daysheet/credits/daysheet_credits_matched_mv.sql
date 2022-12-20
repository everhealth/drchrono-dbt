{{ config(
    materialized = 'view',
    sort = ['created_at', 'practice_group_id', 'doctor_id'],
    auto_refresh = 'true'
) }}
SELECT lit.id
    -- LineItemTransaction
    , lit.ins_paid
    , lit.created_at
    , lit.posted_date
    , lit.code
    , lit.trace_number
    , lit.adjustment
    , lit.adjustment_reason
    , lit.ins_idx
    , lit.doctor_id
    , lit.patient_id
    -- Patient
    , cp.first_name
    , cp.middle_name
    , cp.last_name
    , cp.chart_id
    -- Office
    , co.name
    -- Doctor
    , cd.firstName
    , cd.lastName
    , cd.salutation
    , cd.suffix
    , cd.practice_group_id
    -- Appointment
    , lit.appointment_id
    , ca.examination_room as exam_room_id
    , {{ exam_room_name()}}
    , ca.office_id
    , ca.service_date_start_date
    , ca.service_date_end_date
    , ca.first_billed_date
    , ca.scheduled_time
    , ca.institutional_claim_flag
    , ca.primary_insurer_company
    , ca.primary_insurer_payer_id
    , ca.secondary_insurer_company
    , ca.secondary_insurer_payer_id
    , era.deposit_date
    , era.payer_id
    , era.insurance_name
    , era.total_paid
    , era.trace_number                                   AS era_trace_number
FROM {{ source('chronometer_production', 'billing_lineitemtransaction') }} lit
JOIN {{ source('chronometer_production', 'chronometer_appointment') }} ca
    ON (lit.appointment_id = ca.id)
JOIN {{ source('chronometer_production', 'chronometer_doctor') }} cd
    ON (lit.doctor_id = cd.id)
JOIN {{ source('chronometer_production', 'chronometer_patient') }} cp
    ON (lit.patient_id = cp.id)
JOIN {{ source('chronometer_production', 'chronometer_office') }} co
    ON (ca.office_id = co.id)
JOIN {{ source('chronometer_production', 'billing_eraobject') }} era
    ON (era.id = lit.era_id)
WHERE (
lit.is_archived = False
AND NOT (lit.ins_paid=0 AND lit.adjusted_adjustment_reason IN ('SKIP_SECONDARY', 'DENIAL') )
AND NOT (
    lit.ins_paid = 0
    AND COALESCE( lit.adjusted_adjustment_reason, '' ) = 'PATIENT_RESPONSIBLE'
    AND NOT (lit.adjustment_reason = 1)
)
AND NOT ( era.is_verified IS FALSE OR era.is_archived IS TRUE )
AND NOT (
    COALESCE(lit.adjustment_group_code, '') = 'CO'
    AND lit.ins_idx = 2
)
);
