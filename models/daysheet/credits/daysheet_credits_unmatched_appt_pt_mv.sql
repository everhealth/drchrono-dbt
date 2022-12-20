SELECT lit.id
           -- LineItemTransaction
          , lit.ins_paid
          , lit.created_at
          , lit.posted_date
          , lit.code
          , lit.trace_number
          , lit.adjustment
          , lit.adjustment_reason
          , lit.ins_idx
          , lit.doctor_id
          , lit.patient_id
           -- Patient
          , '' AS first_name
          , '' AS middle_name
          , '' AS last_name
          , '' AS chart_id
           -- Office
          , '' AS name
           -- Doctor
          , cd.firstName
          , cd.lastName
          , cd.salutation
          , cd.suffix
          , cd.practice_group_id
           -- Appointment
          , -1                               AS appointment_id
          , -1                               AS exam_room_id
          , ''							     AS exam_room_name
          , -1                               AS office_id
          , date('1000-01-02')               AS service_date_start_date
          , date('1000-01-01')               AS service_date_end_date
          , '1000-01-02 01:01:01'::timestamp AS first_billed_date
          , '1000-01-01 01:01:01'::timestamp AS scheduled_time
          , 0                                AS institutional_claim_flag
          , ''                               AS primary_insurer_company
          , ''                               AS primary_insurer_payer_id
          , ''                               AS secondary_insurer_company
          , ''                               AS secondary_insurer_payer_id
          -- ERA
          , era.deposit_date
          , era.payer_id
          , era.insurance_name
          , era.total_paid
          , era.trace_number                                   AS era_trace_number
  FROM {{ source('chronometer_production', 'billing_lineitemtransaction') }} lit
      JOIN {{ source('chronometer_production', 'chronometer_doctor') }} cd
          ON (lit.doctor_id = cd.id)
      JOIN {{ source('chronometer_production', 'billing_eraobject') }} era
          ON (era.id = lit.era_id)

  WHERE (
      lit.is_archived = False
      AND NOT (lit.ins_paid=0 AND lit.adjusted_adjustment_reason IN ('SKIP_SECONDARY', 'DENIAL') )
      AND NOT (
          lit.ins_paid = 0
          AND COALESCE( lit.adjusted_adjustment_reason, '' ) = 'PATIENT_RESPONSIBLE'
          AND NOT (lit.adjustment_reason = 1)
      )
      AND NOT ( era.is_verified IS FALSE OR era.is_archived IS TRUE )
      AND NOT (
          COALESCE(lit.adjustment_group_code, '') = 'CO'
          AND lit.ins_idx = 2
      )
  AND lit.appointment_id IS NULL
  AND lit.patient_id IS NULL
  )
