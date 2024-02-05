{{ config(SORT = ['bli_created_at', 'appt_date_of_service', 'practice_group_id', 'doctor_id']) }}

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

    --appt
  , a.*

    -- office
  , {{office_fields( 'o' )}}
    -- patient
  , {{patient_fields( 'p' )}}
  , p.patient_primary_insurance_company

    -- doctor
  , {{doctor_fields( 'd' )}}
  , d.practice_group_id
  , d.doc_verify_era_before_post

FROM {{ ref( 'stg_line_items' ) }} bli
LEFT JOIN {{ ref('stg_appointments') }} a
    ON a.appointment_id = bli.appointment_id
LEFT JOIN {{ ref('stg_doctors') }} d
    ON d.doctor_id = a.doctor_id
LEFT JOIN {{ ref('stg_offices') }} o
    ON o.office_id = a.office_id
LEFT JOIN {{ ref('stg_patients') }} p
    ON cp.id = a.patient_id
WHERE
    bli.appointment_id IS NULL
  OR a.appointment_id IS NOT NULL
