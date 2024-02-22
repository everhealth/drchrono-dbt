SELECT
    id                                                  AS appointment_id
    , appointment_status
    , doctor_id                                         AS fk_doctor_id
    , office_id                                         AS fk_office_id
    , patient_id                                        AS fk_patient_id
    , examination_room
    , updated_at
    , scheduled_time::DATE                              AS date_of_service
    , appointment_profile_id
    , payment_profile
    , billing_status                                    AS appt_billing_status
    , ins1_status
    , ins2_status
    , claim_type
    , submitted
    , latest_payer_claim_status
    , resubmit_claim_flag
    , void_claim_flag
    , primary_insurer_payer_id
    , secondary_insurer_payer_id
    , primary_insurer_company
    , secondary_insurer_company
    , icd_version_number
    , institutional_claim_flag
    , billing_provider_id
    , supervising_provider_id
    --dates
    , updated_at                                        AS appt_updated_at
    , CONVERT_TIMEZONE('PST', 'UTC', first_billed_date) AS first_billed_date
    , CONVERT_TIMEZONE('PST', 'UTC', last_billed_date)  AS last_billed_date
    , CONVERT_TIMEZONE(
        'PST', 'UTC', service_date_start_date
    )                                                   AS service_date_start_date
    , CONVERT_TIMEZONE(
        'PST', 'UTC', service_date_end_date
    )                                                   AS service_date_end_date
    , CONVERT_TIMEZONE('PST', 'UTC', created_at)        AS appt_created_at



    --insurance collapse

    , CASE
        WHEN ins1_status != '' AND ins2_status = '' THEN ins1_status
        WHEN
            billing_status = 'Bill Secondary Insurance'
            OR (
                billing_status = 'Bill Insurance'
                AND ins1_status = 'Coordination of Benefits'
            ) THEN ins2_status
        ELSE ins1_status
    END                                                 AS appt_claim_status


    , CASE
        WHEN ins1_status != '' AND ins2_status = '' THEN primary_insurer_company
        WHEN
            billing_status = 'Bill Secondary Insurance'
            OR (
                billing_status = 'Bill Insurance'
                AND ins1_status = 'Coordination of Benefits'
            ) THEN secondary_insurer_company
        ELSE primary_insurer_company
    END                                                 AS appt_insurance_company



FROM {{ source("chronometer_production", "chronometer_appointment") }}
WHERE
    deleted_flag IS FALSE
    AND appt_is_break IS FALSE
    AND is_demo_data_appointment IS FALSE
    AND _fivetran_deleted IS FALSE
