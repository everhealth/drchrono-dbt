SELECT
    id as line_item_id
  , billing_status
  , code
  , description
  , procedure_type
  , quantity
  , price
  , COALESCE( billed, 0 )
  , allowed
  , adjustment
  , ins1_paid
  , ins2_paid
  , ins3_paid
  , pt_paid
  , balance_ins
  , balance_pt
  , resp_ins
  , resp_pt
  , billing_profile_id
  , CONVERT_TIMEZONE( 'EST', 'UTC', created_at )
  , expected_reimbursement
  , appointment_id
FROM {{source( 'chronometer_scrubbed', 'billing_billinglineitem' ) }}