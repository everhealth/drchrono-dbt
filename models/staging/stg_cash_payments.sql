select
    id as cashpayment_id,
    appointment_id,
    line_item_id,
    posted_date,
    received_date as payment_date,
    created_by_id,
    amount as cashpayment_amount,
    trace_number,
    payment_method,
    doctor_id,
    patient_id,
    parent_id,
    updated_at as cash_updated_at

from {{ source("chronometer_production", "billing_cashpayment") }}
WHERE _fivetran_deleted is false
