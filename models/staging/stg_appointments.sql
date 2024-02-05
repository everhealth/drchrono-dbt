SELECT
    id                                                  AS appointment_id
  , appointment_status
  , scheduled_time::DATE                                AS appt_date_of_service
  , appointment_profile_id                              AS appt_appointment_profile_id
  , appt_is_break                                       AS appt_appt_is_break
  , payment_profile                                     AS appt_payment_profile
  , billing_status                                      AS appt_billing_status
  , ins1_status                                         AS appt_ins1_status
  , ins2_status                                         AS appt_ins2_status
  , CONVERT_TIMEZONE( 'PST', 'UTC', first_billed_date ) AS appt_first_billed_date
  , CONVERT_TIMEZONE( 'PST', 'UTC', last_billed_date )  AS appt_last_billed_date
  , claim_type                                          AS appt_claim_type
  , submitted                                           AS appt_submitted
  , latest_payer_claim_status                           AS appt_latest_payer_claim_status
  , resubmit_claim_flag                                 AS appt_resubmit_claim_flag
  , void_claim_flag                                     AS appt_void_claim_flag
  , primary_insurer_payer_id                            AS appt_primary_insurer_payer_id
  , secondary_insurer_payer_id                          AS appt_secondary_insurer_payer_id
  , primary_insurer_company                             AS appt_primary_insurer_company
  , secondary_insurer_company                           AS appt_secondary_insurer_company
  , CONVERT_TIMEZONE( 'PST', 'UTC', created_at )        AS appt_created_at
  , icd_version_number                                  AS appt_icd_version_number
  , institutional_claim_flag                            AS appt_institutional_claim_flag
  , examination_room                                    AS appt_examination_room
  , billing_provider_id                                 AS appt_billing_provider_id
  , supervising_provider_id                             AS appt_supervising_provider_id
FROM {{source( 'chronometer_scrubbed', 'chronometer_appointment' ) }}
WHERE
      deleted_flag IS FALSE
  AND appt_is_break IS FALSE
  AND is_demo_data_appointment IS FALSE