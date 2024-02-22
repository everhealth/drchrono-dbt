{{
    config(
        SORT=[
            "bli_created_at",
            "date_of_service",
            "practice_group_id",
            "doctor_id",
        ]
    )
}}

select

    a.appointment_id,
    a.appointment_status,
    a.date_of_service,
    a.appointment_profile_id,
    a.payment_profile,
    a.appt_billing_status,
    a.ins1_status,
    a.ins2_status,
    a.first_billed_date,
    a.last_billed_date,
    a.claim_type,
    a.submitted,
    a.latest_payer_claim_status,
    a.resubmit_claim_flag,
    a.void_claim_flag,
    a.primary_insurer_payer_id,
    a.secondary_insurer_payer_id,
    a.primary_insurer_company,
    a.secondary_insurer_company,
    a.appt_created_at,
    a.icd_version_number,
    a.institutional_claim_flag,
    a.examination_room as exam_room_id,
    {{ exam_room_name("a","o") }},
    a.billing_provider_id,
    a.supervising_provider_id,
    li.line_item_id,
    li.li_billing_status,
    li.billing_code,
    li.description,
    li.procedure_type,
    li.quantity,
    li.price,
    li.allowed,
    li.adjustment,
    li.ins1_paid,
    li.ins2_paid,
    li.ins3_paid,
    li.pt_paid,
    li.balance_ins,
    li.balance_pt,
    li.resp_ins,
    li.resp_pt,
    li.billing_profile_id,
    li.expected_reimbursement,
    li.billed,
    li.li_created_at,
    -- office
    {{ office_fields("o") }},
    -- patient
    {{ patient_fields("p") }},
    p.primary_insurance_company,
    case
        when
            a.ins1_status != '' and a.ins2_status = ''
            then p.primary_insurance_plan_type
        when
            a.appt_billing_status = 'Bill Secondary Insurance'
            or (
                a.appt_billing_status = 'Bill Insurance'
                and a.ins1_status = 'Coordination of Benefits'
            ) then p.secondary_insurance_plan_type
        else p.primary_insurance_plan_type
    end as insurance_plan,
    case
        when ins1_status != '' and ins2_status = '' then p.primary_insurance_company
        when
            appt_billing_status = 'Bill Secondary Insurance'
            or (
                appt_billing_status = 'Bill Insurance'
                and ins1_status = 'Coordination of Benefits'
            ) then secondary_insurer_company
        else p.primary_insurance_company
    end as patient_insurance_company,
    
    -- doctor
    {{ doctor_fields() }},
    d.practice_group_id,
    d.verify_era_before_post,
    --updates
    li.li_updated_at, 
    a.appt_updated_at, 
    d.doc_updated_at, 
    o.office_updated_at, 
    p.patient_updated_at

from {{ ref("stg_line_items") }} as li
left join
    {{ ref("stg_appointments") }} as a
    on li.appointment_id = a.appointment_id AND {{ days_ago("a.appt_updated_at", 90) }}
left join {{ ref("stg_doctors") }} as d on a.doctor_id = d.doctor_id and {{ filter_pg("d")}}
left join {{ ref("stg_offices") }} as o on a.office_id = o.office_id
left join {{ ref("stg_patients") }} as p on a.patient_id = p.patient_id
where li.appointment_id is null or a.appointment_id is not null
