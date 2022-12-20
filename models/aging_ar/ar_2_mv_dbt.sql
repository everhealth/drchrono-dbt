{{ config(
    materialized='materialized_view',
    sort=['first_billed_date', 'doctor_id', 'appointment_id', 'patient_id'],
    auto_refresh='true')
}}

SELECT
        -- BLI
        bli.id,
        bli.balance_ins,
        bli.balance_pt,
        bli.billed,
        bli.allowed, -- TODO: We may not need this column?
        bli.price,
        bli.quantity,
        bli.adjustment,
        bli.expected_reimbursement, -- for AR Days
        bli.procedure_type,
        bli.code,
        bli.created_at,
        bli.from_date,
        bli.to_date,
        --ubr.revenue_code,
        -- Appt
        -- IDs for FKs
        bli.appointment_id,
        ca.doctor_id,
        cd.practice_group_id,
        ca.batch_billing_file_id,
        ca.office_id,
        co.name as office_name,
        -- statuses
        ca.billing_status,
        ca.ins1_status,
        ca.ins2_status,
        ca.institutional_claim_flag,
        ca.submitted,
        ca.drchrono_errors,
        ca.emdeon_errors,
        ca.payer_errors,
        -- insurance details
        ca.primary_insurer_payer_id,
        ca.secondary_insurer_payer_id,
        ca.secondary_insurer_company,
        ca.primary_insurer_company,
        -- Appt dates
        ca.scheduled_time,
        ca.last_billed_date,
        ca.first_billed_date,
        ca.service_date_start_date,
        ca.service_date_end_date,
        -- Patient
        ca.patient_id,
        pt.chart_id,
        pt.date_of_birth as dob,
        pt.first_name,
        pt.middle_name,
        pt.last_name,
        pt.suffix,
        pt.nick_name,
        pt.primary_insurance_plan_type,
        pt.secondary_insurance_plan_type,
        CASE
            WHEN pt.preferred_confidential_communication_method = 'Phone' THEN pt.home_phone
            WHEN pt.preferred_confidential_communication_method = 'Work' THEN pt.office_phone
            WHEN pt.preferred_confidential_communication_method = 'Cell' THEN pt.cell_phone
        END as phone,
        pt.patient_payment_profile,
        -- Doctor
        cd.firstName,
        cd.lastName,
        cd.salutation,
        cd.suffix AS doc_suffix

FROM  {{ source('chronometer_production', 'billing_billinglineitem') }} bli
JOIN {{ source('chronometer_production', 'chronometer_appointment') }} ca
    ON (
        bli.appointment_id = ca.id
        AND ca.appointment_status NOT IN ('Cancelled', 'Rescheduled')
        )
JOIN {{ source('chronometer_production', 'chronometer_patient') }} pt
    ON (ca.patient_id = pt.id)
JOIN {{ source('chronometer_production', 'chronometer_doctor') }} cd
    ON (ca.doctor_id = cd.id)
JOIN {{ source('chronometer_production', 'chronometer_office') }} co
    ON (ca.office_id = co.id)

WHERE
        ca.appt_is_break=False
    AND ca.deleted_flag=False
    AND NOT (
        ca.is_special_case_of_recur_series = False
        AND ca.recurring_appointment = True
    )
    AND NOT (
        ca.appointment_status='No Show'
        AND bli.procedure_type in ('C', 'H', 'R')
        )
