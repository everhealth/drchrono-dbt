select
    id as appointment_id,
    appointment_status,
    doctor_id,
    office_id,
    patient_id,
    examination_room,
    updated_at,
    scheduled_time::date as date_of_service,
    appointment_profile_id,
    payment_profile,
    billing_status as appt_billing_status,
    ins1_status,
    ins2_status,
    claim_type,
    submitted,
    latest_payer_claim_status,
    resubmit_claim_flag,
    void_claim_flag,
    primary_insurer_payer_id,
    secondary_insurer_payer_id,
    primary_insurer_company,
    secondary_insurer_company,
    icd_version_number,
    institutional_claim_flag,
    billing_provider_id,
    supervising_provider_id,
    --dates
    updated_at as appt_updated_at,
    convert_timezone('PST', 'UTC', first_billed_date) as first_billed_date,
    convert_timezone('PST', 'UTC', last_billed_date) as last_billed_date,
    convert_timezone(
        'PST', 'UTC', service_date_start_date
    ) as service_date_start_date,
    convert_timezone(
        'PST', 'UTC', service_date_end_date
    ) as service_date_end_date,
    convert_timezone('PST', 'UTC', created_at) as appt_created_at,



    --insurance collapse

    case
        when ins1_status != '' and ins2_status = '' then ins1_status
        when
            billing_status = 'Bill Secondary Insurance'
            or (
                billing_status = 'Bill Insurance'
                and ins1_status = 'Coordination of Benefits'
            ) then ins2_status
        else ins1_status
    end as claim_status,


    case
        when ins1_status != '' and ins2_status = '' then primary_insurer_company
        when
            billing_status = 'Bill Secondary Insurance'
            or (
                billing_status = 'Bill Insurance'
                and ins1_status = 'Coordination of Benefits'
            ) then secondary_insurer_company
        else primary_insurer_company
    end as appt_insurance_company



from {{ source("chronometer_production", "chronometer_appointment") }}
where
    deleted_flag is false
    and appt_is_break is false
    and is_demo_data_appointment is false
    and _fivetran_deleted is false
