SELECT
            lit.id
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
          , cp.first_name
          , cp.middle_name
          , cp.last_name
          , cp.chart_id
           -- Office
          , co.name
           -- Doctor
          , cd.firstName
          , cd.lastName
          , cd.salutation
          , cd.suffix
          , cd.practice_group_id
           -- Appointment
          , lit.appointment_id
          , ca.examination_room as exam_room_id
          , decode(ca.examination_room,
            		1, co.exam_room_1_name,
                    2, co.exam_room_2_name,
                    3, co.exam_room_3_name,
                    4, co.exam_room_4_name,
                    5, co.exam_room_5_name,
                    6, co.exam_room_6_name,
                    7, co.exam_room_7_name,
                    8, co.exam_room_8_name,
                    9, co.exam_room_9_name,
                    10, co.exam_room_10_name,
                    11, co.exam_room_11_name,
                    12, co.exam_room_12_name,
                    13, co.exam_room_13_name,
                    14, co.exam_room_14_name,
                    15, co.exam_room_15_name,
                    '') AS exam_room_name
          , ca.office_id
          , ca.service_date_start_date
          , ca.service_date_end_date
          , ca.first_billed_date
          , ca.scheduled_time
          , ca.institutional_claim_flag
          , ca.primary_insurer_company
          , ca.primary_insurer_payer_id
          , ca.secondary_insurer_company
          , ca.secondary_insurer_payer_id
          -- ERA
          , date('1000-01-02') AS deposit_date -- date
          , ''::varchar(96) as payer_id -- varchar(96)
          , ''::varchar(384) AS insurance_name -- varchar(384)
          , -500000.01::numeric(12,2) AS total_paid -- numeric(12,2)
          , ''::varchar(192) AS era_trace_number   -- varchar(192)

  FROM {{ source('chronometer_production', 'billing_lineitemtransaction') }} lit
      JOIN {{ source('chronometer_production', 'chronometer_appointment') }} ca
        ON (lit.appointment_id = ca.id)
      JOIN {{ source('chronometer_production', 'chronometer_doctor') }} cd
        ON (lit.doctor_id = cd.id)
      JOIN {{ source('chronometer_production', 'chronometer_patient') }} cp
        ON (lit.patient_id = cp.id)
      JOIN {{ source('chronometer_production', 'chronometer_office') }} co
        ON (ca.office_id = co.id)
  WHERE (
      lit.is_archived = False
      AND NOT (lit.ins_paid=0 AND lit.adjusted_adjustment_reason IN ('SKIP_SECONDARY', 'DENIAL') )
      AND NOT (
          lit.ins_paid = 0
          AND COALESCE( lit.adjusted_adjustment_reason, '' ) = 'PATIENT_RESPONSIBLE'
          AND NOT (lit.adjustment_reason = 1)
      )
      AND NOT (
          COALESCE(lit.adjustment_group_code, '') = 'CO'
          AND lit.ins_idx = 2
      )
      AND lit.era_id IS NULL
  )
