select
    bcp.id as cashpayment_id,
    bcp.appointment_id,
    bcp.line_item_id,
    bcp.posted_date,
    bcp.received_date as payment_date,
    bcp.created_by_id,
    bcp.amount,
    bcp.trace_number,
    bcp.payment_method,
    bcp.doctor_id as doctor_id,
    bcp.patient_id as patient_id,
    bcp.parent_id

from {{ source("chronometer_scrubbed", "billing_cashpayment") }} as bcp
