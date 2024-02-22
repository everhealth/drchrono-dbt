SELECT
    id               AS cashpayment_id
    , appointment_id AS fk_appointment_id
    , line_item_id   AS fk_line_item_id
    , posted_date
    , received_date  AS payment_date
    , created_by_id
    , amount         AS cashpayment_amount
    , trace_number
    , payment_method
    , doctor_id      AS fk_doctor_id
    , patient_id     AS fk_patient_id
    , parent_id
    , updated_at     AS cash_updated_at

FROM {{ source("chronometer_production", "billing_cashpayment") }}
WHERE _fivetran_deleted IS FALSE
