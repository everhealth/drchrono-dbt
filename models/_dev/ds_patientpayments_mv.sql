{{ config(materialized='view') }}


-- DROP MATERIALIZED VIEW IF EXISTS temp.daysheet_patientpayments_mv__test;


  --omit diststyle to rely on Redshift's AUTO distribution style

    /*
     This table should contain all cashpayments excluding line items belonging to appointments that are deleted, breaks, or demo data.
     Left join and filter conditions are to allow cashpayments with a NULL appointment id in the source table. This eliminates the need for a separate insert unmatched cashpayments task.
     Cashpayments without appointment_id are typically rebalances that occur when a cashpayment is assigned to a specific appointment/line item.
     */
--     WITH
--         appt_filtered AS (
--                          SELECT

    SELECT
        bcp.id  --           AS cash_payment_id
        -- bcp.patient_id --     AS cash_patient_id
       , chronometer_production.billing_cashpayment.amount       --  AS cash_amount
      , chronometer_production.billing_cashpayment.payment_method -- AS cash_payment_method
      , chronometer_production.billing_cashpayment.trace_number -- AS cash_trace_number
      , chronometer_production.billing_cashpayment.appointment_id

      , chronometer_production.billing_cashpayment.posted_date -- convert_timezone('EST', 'UTC', bcp.posted_date)    -- AS cash_posted_date
      -- , bcp.created_at -- , convert_timezone('EST', 'UTC', bcp.created_at)     -- AS cash_created_at
      , chronometer_production.billing_cashpayment.received_date -- convert_timezone('EST', 'UTC', chronometer_production.billing_cashpayment.received_date)  -- AS cash_received_date
      , chronometer_production.billing_cashpayment.payment_type  -- AS cash_payment_type
      , chronometer_production.billing_cashpayment.line_item_id  -- AS cash_line_item_id
      , chronometer_production.billing_cashpayment.notes        --  AS cash_notes
      -- , CASE
      --       WHEN bcp.notes LIKE 'Counter Balance%'
      --           THEN TRUE
      --           ELSE FALSE
      --   END                AS is_counter_balance

       ,  chronometer_production.chronometer_appointment.id        --                               AS appointment_id
       ,  chronometer_production.chronometer_appointment.doctor_id
       , chronometer_production.chronometer_appointment.appointment_status
       , chronometer_production.chronometer_appointment.is_demo_data_appointment
       , chronometer_production.chronometer_appointment.deleted_flag
       , chronometer_production.chronometer_appointment.scheduled_time  --convert_timezone('EST', 'UTC', chronometer_production.chronometer_appointment.scheduled_time)       --             AS appt_scheduled_time
       , chronometer_production.chronometer_appointment.appointment_profile_id --            AS appt_appointment_profile_id
       , chronometer_production.chronometer_appointment.appt_is_break  --                     AS appt_appt_is_break
       , chronometer_production.chronometer_appointment.payment_profile --                   AS appt_payment_profile
       , chronometer_production.chronometer_appointment.billing_status  --                    AS appt_billing_status
       , chronometer_production.chronometer_appointment.ins1_status  --                       AS appt_ins1_status
       , chronometer_production.chronometer_appointment.ins2_status  --                       AS appt_ins2_status
       , chronometer_production.chronometer_appointment.first_billed_date  --                 AS appt_first_billed_date
       , chronometer_production.chronometer_appointment.last_billed_date  --                 AS appt_last_billed_date
       , chronometer_production.chronometer_appointment.claim_type  --                        AS appt_claim_type
       , chronometer_production.chronometer_appointment.submitted  --                         AS appt_submitted
       , chronometer_production.chronometer_appointment.latest_payer_claim_status  --         AS appt_latest_payer_claim_status
       , chronometer_production.chronometer_appointment.icd10_diagnosis_json  --              AS appt_icd10_diagnosis_json
       , chronometer_production.chronometer_appointment.pre_authorization_approval_number  -- AS appt_pre_authorization_approval_number
       , chronometer_production.chronometer_appointment.resubmit_claim_flag  --               AS appt_resubmit_claim_flag
       , chronometer_production.chronometer_appointment.void_claim_flag  --                   AS appt_void_claim_flag
       , chronometer_production.chronometer_appointment.primary_insurer_payer_id  --          -- AS appt_primary_insurer_payer_id
       , chronometer_production.chronometer_appointment.secondary_insurer_payer_id         -- AS appt_secondary_insurer_payer_id
       , chronometer_production.chronometer_appointment.primary_insurer_company            -- AS appt_primary_insurer_company
       , chronometer_production.chronometer_appointment.secondary_insurer_company         -- AS appt_secondary_insurer_company
       , chronometer_production.chronometer_appointment.created_at -- , convert_timezone('EST', 'UTC', chronometer_production.chronometer_appointment.created_at)                         -- AS appt_created_at
       , chronometer_production.chronometer_appointment.icd9_diagnosis                     -- AS appt_icd9_diagnosis
       , chronometer_production.chronometer_appointment.icd_version_number                 -- AS appt_icd_version_number
       , chronometer_production.chronometer_appointment.institutional_claim_flag           -- AS appt_institutional_claim_flag
       , chronometer_production.chronometer_appointment.examination_room                   -- AS appt_examination_room
         -- chronometer_office
       , chronometer_production.chronometer_appointment.office_id
       , chronometer_production.chronometer_office.name                              -- AS office_name
       , chronometer_production.chronometer_office.state                             -- AS office_state
       , chronometer_production.chronometer_office.facility_name                     -- AS office_facility_name
       , chronometer_production.chronometer_office.facility_code                     -- AS office_facility_code
         -- chronometer_patient
       , chronometer_production.chronometer_appointment.patient_id
       , chronometer_production.chronometer_patient.chart_id                        --  AS patient_chart_id
       , chronometer_production.chronometer_patient.patient_payment_profile        --
       , chronometer_production.chronometer_patient.primary_insurance_company        -- AS patient_primary_insurance_company
       , chronometer_production.chronometer_patient.first_name                       -- AS patient_first_name
       , chronometer_production.chronometer_patient.middle_name                     --  AS patient_middle_name
       , chronometer_production.chronometer_patient.last_name                        -- AS patient_last_name
         -- chronometer_doctor
       -- , chronometer_production.chronometer_doctor.id                                                                                   AS doctor_id
       , chronometer_production.chronometer_doctor.practice_group_id
       , chronometer_production.chronometer_doctor.firstName                        --                                                    AS doc_firstname
       , chronometer_production.chronometer_doctor.middleName                       --                                                    AS doc_middlename
       , chronometer_production.chronometer_doctor.lastName                         --                                                    AS doc_lastname
       , chronometer_production.chronometer_doctor.salutation                       --                                                    AS doc_salutation
       , chronometer_production.chronometer_doctor.suffix                           --                                                    AS doc_suffix
       , chronometer_production.chronometer_doctor.specialty                        --                                                    AS doc_specialty
       , chronometer_production.chronometer_doctor.is_test_user                     --                                                   AS doc_is_test_user
       , chronometer_production.chronometer_doctor.is_paid_user                     --                                                    AS doc_is_paid_user
       , chronometer_production.chronometer_doctor.is_realtime_eligibility_user     --                                                    AS doc_is_realtime_eligibility_user
       --, COALESCE(chronometer_production.chronometer_doctor.is_account_suspended, FALSE)    --                                            AS doc_hard_suspended
       , chronometer_production.chronometer_doctor.enable_emdeon_billing_submission         --                                            AS doc_enable_emdeon_billing_submission
       , chronometer_production.chronometer_doctor.enable_ihcfa_billing_submission          --                                            AS doc_enable_ihcfa_billing_submission
       , chronometer_production.chronometer_doctor.enable_gateway_billing_submission        --                                            AS doc_enable_gateway_billing_submission
       , chronometer_production.chronometer_doctoroptions.enable_waystar_billing_submission       --                                             AS doc_enable_waystar_billing_submission
       , chronometer_production.chronometer_doctor.enable_medical_billing                   --                                            AS doc_enable_medical_billing
       , chronometer_production.chronometer_doctor.enable_professional_billing              --                                            AS doc_enable_professional_billing
       , chronometer_production.chronometer_doctor.organization_name                        --                                            AS doc_organization_name
       , chronometer_production.chronometer_doctor.auto_post_eras                           --                                            AS doc_auto_post_eras
       , chronometer_production.chronometer_doctor.verify_era_before_post                   --                                            AS doc_verify_era_before_post
       --, chronometer_production.chronometer_doctor.dr_organization_name                                                                 AS doc_dr_organization_name
       , chronometer_production.chronometer_doctor.billing_srb_rate                         --                                            AS doc_billing_srb_rate
       -- , chronometer_production.chronometer_doctor.billing_monthly_minimum                                                              AS doc_billing_monthly_minimum
       , chronometer_production.chronometer_doctor.official_office_state                    --                                            AS doc_official_office_state
       -- , cd.credit_card_plan_type                                                                AS doc_credit_card_plan_type
       -- , cd.credit_card_plan_price                                                               AS doc_credit_card_plan_price
       -- , cd.plan_type_decoded                                                                    AS doc_plan_type_decoded
     FROM chronometer_production.chronometer_appointment
     JOIN chronometer_production.chronometer_doctor
          ON chronometer_production.chronometer_doctor.id = chronometer_production.chronometer_appointment.doctor_id
     JOIN chronometer_production.billing_cashpayment
          ON chronometer_production.chronometer_doctor.id = chronometer_production.billing_cashpayment.doctor_id
          AND chronometer_production.chronometer_appointment.id = chronometer_production.billing_cashpayment.appointment_id
     JOIN chronometer_production.chronometer_doctoroptions
          ON chronometer_production.chronometer_doctor.id = chronometer_production.chronometer_doctoroptions.doctor_id AND chronometer_production.chronometer_doctoroptions.id IS NOT NULL
     JOIN chronometer_production.chronometer_office
          ON chronometer_production.chronometer_office.id = chronometer_production.chronometer_appointment.office_id
     JOIN chronometer_production.chronometer_patient
          ON chronometer_production.chronometer_patient.id = chronometer_production.chronometer_appointment.patient_id
     WHERE
           chronometer_production.chronometer_appointment.deleted_flag IS FALSE
       AND chronometer_production.chronometer_appointment.appt_is_break IS FALSE
       AND chronometer_production.chronometer_appointment.is_demo_data_appointment IS FALSE
       AND chronometer_production.chronometer_patient.is_demo_data_patient IS FALSE
