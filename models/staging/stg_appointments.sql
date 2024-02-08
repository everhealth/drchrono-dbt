select
    id as appointment_id,
    appointment_status,
    doctor_id,
    office_id,
    patient_id,
    examination_room,
    updated_at,
    scheduled_time::date as appt_date_of_service,
    appointment_profile_id as appt_appointment_profile_id,
    appt_is_break as appt_appt_is_break,
    payment_profile as appt_payment_profile,
    billing_status as appt_billing_status,
    ins1_status as appt_ins1_status,
    ins2_status as appt_ins2_status,
    claim_type as appt_claim_type,
    submitted as appt_submitted,
    latest_payer_claim_status as appt_latest_payer_claim_status,
    resubmit_claim_flag as appt_resubmit_claim_flag,
    void_claim_flag as appt_void_claim_flag,
    primary_insurer_payer_id as appt_primary_insurer_payer_id,
    secondary_insurer_payer_id as appt_secondary_insurer_payer_id,
    primary_insurer_company as appt_primary_insurer_company,
    secondary_insurer_company as appt_secondary_insurer_company,
    icd_version_number as appt_icd_version_number,
    institutional_claim_flag as appt_institutional_claim_flag,
    examination_room as appt_examination_room,
    billing_provider_id as appt_billing_provider_id,
    supervising_provider_id as appt_supervising_provider_id,
    convert_timezone('PST', 'UTC', first_billed_date) as appt_first_billed_date,
    convert_timezone('PST', 'UTC', last_billed_date) as appt_last_billed_date,
    convert_timezone('PST', 'UTC', service_date_start_date) as appt_service_date_start_date,
    convert_timezone('PST', 'UTC', service_date_end_date) as appt_service_date_end_date,
    convert_timezone('PST', 'UTC', created_at) as appt_created_at

from {{ source("chronometer_scrubbed", "chronometer_appointment") }}
where
    deleted_flag is false
    and appt_is_break is false
    and is_demo_data_appointment is false

