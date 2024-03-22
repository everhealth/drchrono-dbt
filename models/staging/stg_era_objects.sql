SELECT
    id                                             AS era_id
    , payer_id                                     AS era_payer_id
    , insurance_name                               AS era_insurance_name
    , total_paid                                   AS era_total_paid
    , is_verified                                  AS era_is_verified
    , is_eob
    , is_archived                                  AS era_is_archived
    , posted_date                                  AS era_posted_date   -- stored in EST TZ
    , created_at                                   AS era_created_at    -- stored in EST TZ
    , updated_at                                   AS era_updated_at    -- stored in EST TZ
    , deposit_date                                 AS era_deposit_date  -- stored in EST TZ
FROM {{ source("chronometer_production", "billing_eraobject") }}
WHERE _fivetran_deleted IS FALSE
