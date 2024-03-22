SELECT
    id                                           AS line_item_id
    , billing_status                             AS li_billing_status
    , code                                       AS billing_code
    , description
    , procedure_type
    , quantity
    , price
    , allowed                                    AS li_allowed
    , adjustment                                 AS li_adjustment
    , ins1_paid
    , ins2_paid
    , ins3_paid
    , pt_paid
    , balance_ins
    , balance_pt
    , resp_ins
    , resp_pt
    , billing_profile_id
    , expected_reimbursement
    , modifiers_json
    , diagnosis_pointers_json
    , denied_flag
    , appointment_id                             AS li_appointment_id
    , COALESCE(billed, 0)                        AS billed
    , created_at                                 AS li_created_at
    , updated_at                                 AS li_updated_at
FROM {{ source("chronometer_production", "billing_billinglineitem") }}
WHERE _fivetran_deleted IS FALSE
