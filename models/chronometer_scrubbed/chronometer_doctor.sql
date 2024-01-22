{{ config(MATERIALIZED='table', profile='scrubbed') }}

SELECT
    id
  , created_at
  , middlename
  , enable_sales_person_features
  , is_realtime_eligibility_user
  , deleted_flag
  , practice_group_id
  , enable_medical_biller_features
  , npi_number
  , suffix
  , enable_medical_billing
  , enable_professional_billing
  , ichronoapikey
  , line_item_billing
  , practice_group_admin_id
  , billing_notes
  , is_free_widget_user
  , credit_card_plan_type
  , specialty
  , is_beta_user
  , NULL AS license_number
  , job_title
  , supervising_provider_flag
  , firstname
  , is_test_user
  , salutation
  , lastname
  , is_paid_user
  , organization_name
  , _fivetran_deleted
  , _fivetran_synced
FROM {{ source( 'chronometer_production', 'chronometer_doctor' ) }}