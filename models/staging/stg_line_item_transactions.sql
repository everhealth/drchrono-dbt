SELECT
    id               AS line_item_transaction_id
    , line_item_id   AS lit_line_item_id
    , era_id         AS lit_era_id
    , appointment_id AS lit_appointment_id
    , trace_number
    , claim_status   AS lit_claim_status
    , adjustment_group_code
    , allowed        AS lit_allowed
    , adjustment     AS lit_adjustment
    , ins_paid
    , adjustment_reason
    , adjusted_adjustment_reason
    , code
    , status         AS lit_status
    , is_archived    AS lit_is_archived
    , ins_idx
    , updated_at     AS lit_updated_at
    , posted_date    AS lit_posted_date  -- stored in EST TZ
    , created_at     AS lit_created_at   -- stored in EST TZ
FROM {{ source("chronometer_production", "billing_lineitemtransaction") }}
WHERE _fivetran_deleted IS FALSE
