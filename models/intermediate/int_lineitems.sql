{{ config(SORT = ['bli_created_at', 'appt_scheduled_time', 'practice_group_id', 'doctor_id']) }}

WITH
    appt_filtered AS (
                     SELECT
                         da.id                                                  AS appointment_id
                       , da.appointment_status
                       , da.is_demo_data_appointment                            AS appt_is_demo_data_appointment
                       , da.deleted_flag                                        AS appt_deleted_flag
                       , da.scheduled_time                                      AS appt_scheduled_time_raw
                       , da.scheduled_time                                      AS appt_scheduled_time
                       , da.appointment_profile_id                              AS appt_appointment_profile_id
                       , da.appt_is_break                                       AS appt_appt_is_break
                       , da.payment_profile                                     AS appt_payment_profile
                       , da.billing_status                                      AS appt_billing_status
                       , da.ins1_status                                         AS appt_ins1_status
                       , da.ins2_status                                         AS appt_ins2_status
                       , CONVERT_TIMEZONE( 'PST', 'UTC', da.first_billed_date ) AS appt_first_billed_date
                       , CONVERT_TIMEZONE( 'PST', 'UTC', da.last_billed_date )  AS appt_last_billed_date
                       , da.claim_type                                          AS appt_claim_type
                       , da.submitted                                           AS appt_submitted
                       , da.latest_payer_claim_status                           AS appt_latest_payer_claim_status
                       , da.resubmit_claim_flag                                 AS appt_resubmit_claim_flag
                       , da.void_claim_flag                                     AS appt_void_claim_flag
                       , da.primary_insurer_payer_id                            AS appt_primary_insurer_payer_id
                       , da.secondary_insurer_payer_id                          AS appt_secondary_insurer_payer_id
                       , da.primary_insurer_company                             AS appt_primary_insurer_company
                       , da.secondary_insurer_company                           AS appt_secondary_insurer_company
                       , CONVERT_TIMEZONE( 'PST', 'UTC', da.created_at )        AS appt_created_at
                       , da.icd_version_number                                  AS appt_icd_version_number
                       , da.institutional_claim_flag                            AS appt_institutional_claim_flag
                       , da.examination_room                                    AS appt_examination_room
                       , da.billing_provider_id                                 AS appt_billing_provider_id
                       , da.supervising_provider_id                             AS appt_supervising_provider_id
                         -- chronometer_office
                       , da.office_id
                       , co.name                                                AS office_name
                       , co.state                                               AS office_state
                       , co.facility_name                                       AS office_facility_name
                       , co.facility_code                                       AS office_facility_code
                         -- chronometer_patient
                       , da.patient_id
                       , cp.chart_id                                            AS patient_chart_id
                       , cp.patient_payment_profile
                       , cp.primary_insurance_company                           AS patient_primary_insurance_company
                       , cp.first_name                                          AS patient_first_name
                       , cp.middle_name                                         AS patient_middle_name
                       , cp.last_name                                           AS patient_last_name
                         -- chronometer_doctor
                       , cd.id                                                  AS doctor_id
                       , cd.practice_group_id

                     FROM {{source( 'chronometer_scrubbed', 'chronometer_appointment' ) }} da
					 JOIN {{source( 'chronometer_scrubbed', 'chronometer_doctor' ) }} cd
                     ON cd.id = da.doctor_id
                         JOIN {{source( 'chronometer_scrubbed', 'chronometer_office' ) }} co
                         ON co.id = da.office_id
                         JOIN {{source( 'chronometer_scrubbed', 'chronometer_patient' ) }} cp
                         ON cp.id = da.patient_id
                     WHERE
                         da.deleted_flag IS FALSE
                       AND da.appt_is_break IS FALSE
                       AND da.is_demo_data_appointment IS FALSE
                       AND cp.is_demo_data_patient IS FALSE
                     )

SELECT
    bli.id                                           AS line_item_id
  , bli.billing_status                               AS bli_billing_status
  , bli.code                                         AS bli_code
  , bli.description                                  AS bli_description
  , bli.procedure_type                               AS bli_procedure_type
  , bli.quantity                                     AS bli_quantity
  , bli.price                                        AS bli_price
  , COALESCE( bli.billed, 0 )                        AS bli_billed
  , bli.allowed                                      AS bli_allowed
  , bli.adjustment                                   AS bli_adjustment
  , bli.ins1_paid                                    AS bli_ins1_paid
  , bli.ins2_paid                                    AS bli_ins2_paid
  , bli.ins3_paid                                    AS bli_ins3_paid
  , bli.pt_paid                                      AS bli_pt_paid
  , bli.balance_ins                                  AS bli_balance_ins
  , bli.balance_pt                                   AS bli_balance_pt
  , bli.resp_ins                                     AS bli_resp_ins
  , bli.resp_pt                                      AS bli_resp_pt
  , bli.billing_profile_id                           AS bli_billing_profile_id
  , CONVERT_TIMEZONE( 'EST', 'UTC', bli.created_at ) AS bli_created_at
  , bli.expected_reimbursement                       AS bli_expected_reimbursement
  , af.*
FROM {{source( 'chronometer_scrubbed', 'billing_billinglineitem' ) }} bli
LEFT JOIN appt_filtered af
		  USING ( appointment_id )
WHERE
    bli.appointment_id IS NULL
  OR af.appointment_id IS NOT NULL
