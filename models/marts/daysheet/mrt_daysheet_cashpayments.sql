{{ config(SORT=["doctor_id", "posted_date", "scheduled_time"]) }}


select
    -- CashPayment
    bcp.*,
    -- Patient
    {{ patient_fields("p") }},
    -- Doctor
    {{ doctor_fields("d") }},
    d.practice_group_id,
    -- office
    {{ office_fields("o") }}
    -- Appointment
    -- exam_room: ID and NAME
    a.examination_room as exam_room_id,
    {{ exam_room_name() }},
    a.service_date_start_date,
    a.service_date_end_date,
    a.first_billed_date,
    a.scheduled_time,
    a.institutional_claim_flag,
    bli.code as billing_code
from {{ ref("stg_cash_payments") }} as bcp
left join
    {{ ref("stg_appointments") }} as a
    on bcp.appointment_id = a.appointment_id
left join {{ ref("stg_doctors") }} as d on a.doctor_id = d.doctor_id
left join {{ ref("stg_patients") }} as p on bcp.patient_id = p.patient_id
left join {{ ref("stg_offices") }} as o on a.office_id = o.office_id
left join
    {{ ref("stg_line_items") }} as bli
    on bcp.line_item_id = bli.line_item_id

where
    amount != 0
    and coalesce(a.appointment_status, '') not in ('Cancelled', 'Rescheduled')
