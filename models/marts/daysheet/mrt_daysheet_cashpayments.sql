{{ config(SORT=["doctor_id", "posted_date", "appt_date_of_service"]) }}


select
    -- CashPayment
    bcp.cashpayment_id,
    bcp.appointment_id,
    bcp.line_item_id,
    bcp.posted_date,
    bcp.payment_date,
    bcp.created_by_id,
    bcp.amount,
    bcp.trace_number,
    bcp.payment_method,
    bcp.parent_id,
    -- Patient
    {{ patient_fields("p") }},
    -- Doctor
    {{ doctor_fields("d") }},
    d.practice_group_id,
    -- office
    {{ office_fields("o") }},
    -- Appointment
    -- exam_room: ID and NAME
    a.examination_room as exam_room_id,
    {{ exam_room_name("a","o") }},
    a.appt_service_date_start_date,
    a.appt_service_date_end_date,
    a.appt_first_billed_date,
    a.appt_date_of_service,
    a.appt_institutional_claim_flag,
    bli.code as billing_code,
    appt_primary_insurer_company as ins_info_name,
    appt_primary_insurer_payer_id as ins_info_payer_id
from {{ ref("stg_cash_payments") }} as bcp
left join
    {{ ref("stg_appointments") }} as a
    on bcp.appointment_id = a.appointment_id
left join
    {{ ref("stg_doctors") }} as d
    on coalesce(a.doctor_id, bcp.doctor_id) = d.doctor_id
left join
    {{ ref("stg_patients") }} as p
    on bcp.patient_id = p.patient_id
left join {{ ref("stg_offices") }} as o on a.office_id = o.office_id
left join
    {{ ref("stg_line_items") }} as bli
    on bcp.line_item_id = bli.line_item_id

where
    bcp.amount != 0
    and coalesce(a.appointment_status, '') not in ('Cancelled', 'Rescheduled')
    and datediff(day, GREATEST(bcp.posted_date, bcp.payment_date), current_date) < 365
{{ apply_limit_if_test() }}
