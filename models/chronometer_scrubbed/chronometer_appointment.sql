{{ config(MATERIALIZED='table', profile='scrubbed', SCHEMA='chronometer_scrubbed') }}

SELECT
    id
  , is_demo_data_appointment
  , patient_payment
  , primary_payer_claim_id_number
  , institutional_claim_flag
  , patient_id
  , appointment_status
  , created_at
  , service_date_start_date
  , icd_version_number
  , last_related_visit_date
  , created_by_id
  , resubmit_claim_flag
  , initial_visit_date
  , examination_room
  , onset_date
  , scheduled_time
  , original_doctor_id
  , deleted_flag
  , void_claim_flag
  , last_billed_date
  , resubmit_claim_original_id
  , NULL AS billing_note
  , submitted
  , billing_provider_id
  , most_recent_secondary_era_id
  , recurring_appointment
  , billing_status
  , supervising_provider_id
  , claim_type
  , latest_payer_claim_status
  , appointment_profile_id
  , original_base_recurring_appointment_id
  , original_patient_id
  , most_recent_era_id
  , payment_profile
  , recur_end_date
  , original_batch_billing_file_id
  , secondary_insurer_company
  , is_online_scheduled
  , primary_insurer_company
  , is_special_case_of_recur_series
  , most_recent_primary_era_id
  , original_scheduled_time
  , service_date_end_date
  , first_billed_date
  , updated_at
  , office_id
  , doctor_id
  , ins1_status
  , NULL AS notes
  , NULL AS reason
  , number_of_times_to_recur
  , recur_start_date
  , primary_insurer_payer_id
  , allow_overlapping
  , good_faith_estimate_id
  , ins2_status
  , appt_is_break
  , infinite_series
  , original_office_id
  , secondary_insurer_payer_id
  , text_procedures
  , _fivetran_deleted
  , _fivetran_synced
FROM {{ source( 'chronometer_production', 'chronometer_appointment' ) }}