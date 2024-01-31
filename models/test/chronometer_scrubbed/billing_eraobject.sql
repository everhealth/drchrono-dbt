SELECT
    id
  , payment_method
  , is_eob
  , is_archived
  , created_at
  , payer_id
  , updated_at
  , total_paid
  , is_verified
  , NULL AS trace_number
  , doctor_id
  , insurance_name
  , posted_date
  , deposit_date
  , uploaded_era_id
  , _fivetran_deleted
  , _fivetran_synced
FROM {{ source( 'chronometer_production', 'billing_eraobject' ) }}