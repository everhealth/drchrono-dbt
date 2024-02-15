{{
    config(
        SORT=[
            "bli_created_at",
            "appt_date_of_service",
            "practice_group_id",
            "doctor_id",
        ]
    )
}}

select

    a.appointment_id,
    a.appointment_status,
    a.appt_date_of_service,
    a.appt_appointment_profile_id,
    a.appt_appt_is_break,
    a.appt_payment_profile,
    a.appt_billing_status,
    a.appt_ins1_status,
    a.appt_ins2_status,
    a.appt_first_billed_date,
    a.appt_last_billed_date,
    a.appt_claim_type,
    a.appt_submitted,
    a.appt_latest_payer_claim_status,
    a.appt_resubmit_claim_flag,
    a.appt_void_claim_flag,
    a.appt_primary_insurer_payer_id,
    a.appt_secondary_insurer_payer_id,
    a.appt_primary_insurer_company,
    a.appt_secondary_insurer_company,
    a.appt_created_at,
    a.appt_icd_version_number,
    a.appt_institutional_claim_flag,
    a.appt_examination_room as exam_room_id,
    {{ exam_room_name("a","o") }},
    a.appt_billing_provider_id,
    a.appt_supervising_provider_id,
    bli.line_item_id,
    bli.billing_status as bli_billing_status,
    bli.code as billing_code,
    bli.description as bli_description,
    bli.procedure_type as bli_procedure_type,
    bli.quantity as bli_quantity,
    bli.price as bli_price,
    bli.allowed as bli_allowed,
    bli.adjustment as bli_adjustment,
    bli.ins1_paid as bli_ins1_paid,
    bli.ins2_paid as bli_ins2_paid,
    bli.ins3_paid as bli_ins3_paid,
    bli.pt_paid as bli_pt_paid,
    bli.balance_ins as bli_balance_ins,
    bli.balance_pt as bli_balance_pt,
    bli.resp_ins as bli_resp_ins,
    bli.resp_pt as bli_resp_pt,
    bli.billing_profile_id as bli_billing_profile_id,
    bli.expected_reimbursement as bli_expected_reimbursement,
    bli.billed as bli_billed,
    -- appt
    bli.created_at as bli_created_at,
    -- office
    {{ office_fields("o") }},
    -- patient
    {{ patient_fields("p") }},
    p.patient_primary_insurance_company,
    -- doctor
    {{ doctor_fields() }},
    d.practice_group_id,
    d.doc_verify_era_before_post

from {{ ref("stg_line_items") }} as bli
left join
    {{ ref("stg_appointments") }} as a
    on bli.appointment_id = a.appointment_id {{ days_ago("a.updated_at") }}
left join {{ ref("stg_doctors") }} as d on a.doctor_id = d.doctor_id and {{ filter_pg("d")}}
left join {{ ref("stg_offices") }} as o on a.office_id = o.office_id
left join {{ ref("stg_patients") }} as p on a.patient_id = p.patient_id
where bli.appointment_id is null or a.appointment_id is not null
