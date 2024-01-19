{{ config(MATERIALIZED='table', profile='scrubbed') }}

SELECT
    id
  , payment_type
  , LEFT( notes, 1 ) AS notes
  , received_date
  , practice_group_id
  , appointment_id
  , line_item_id
  , is_counter_balance
  , updated_at
  , NULL             AS trace_number
  , parent_id
  , patient_id
  , posted_date
  , created_by_id
  , amount
  , payment_method
  , created_at
  , available
  , payment_transaction_type
  , doctor_id
  , is_primary
  , origin_id
  , _fivetran_deleted
  , _fivetran_synced
FROM {{ source( 'chronometer_production', 'billing_cashpayment' ) }}