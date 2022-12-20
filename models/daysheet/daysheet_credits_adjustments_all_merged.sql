SELECT merged_credits_mvs.id, merged_credits_mvs.ins_paid, merged_credits_mvs.created_at,
           merged_credits_mvs.posted_date, merged_credits_mvs.code, merged_credits_mvs.trace_number,
           merged_credits_mvs.adjustment, merged_credits_mvs.adjustment_reason, merged_credits_mvs.ins_idx,
           merged_credits_mvs.doctor_id, merged_credits_mvs.patient_id, merged_credits_mvs.first_name,
           merged_credits_mvs.middle_name, merged_credits_mvs.last_name, merged_credits_mvs.chart_id,
           merged_credits_mvs.name, merged_credits_mvs.firstname, merged_credits_mvs.lastname,
           merged_credits_mvs.salutation, merged_credits_mvs.suffix,
           merged_credits_mvs.practice_group_id,
           nvl2(CASE
                  WHEN ( merged_credits_mvs.appointment_id < 0 ) = true
                  THEN NULL::boolean
                  ELSE merged_credits_mvs.appointment_id < 0
                END, merged_credits_mvs.appointment_id, NULL::integer)     AS appointment_id,
		   merged_credits_mvs.exam_room_id,
           merged_credits_mvs.exam_room_name,
           nvl2(CASE
                 WHEN ( merged_credits_mvs.office_id < 0 ) = true
                 THEN NULL::boolean
                 ELSE merged_credits_mvs.office_id < 0
                END, merged_credits_mvs.office_id, NULL::integer) AS office_id,
           nvl2(CASE
                 WHEN ( merged_credits_mvs.service_date_start_date < '1200-01-01'::date ) = true
                 THEN NULL::boolean
                 ELSE merged_credits_mvs.service_date_start_date < '1200-01-01'::date
                END, merged_credits_mvs.service_date_start_date, NULL::date)        AS service_date_start_date,
           nvl2(CASE
                 WHEN ( merged_credits_mvs.service_date_end_date < '1200-01-01'::date ) = true
                 THEN NULL::boolean
                 ELSE merged_credits_mvs.service_date_end_date < '1200-01-01'::date
                END, merged_credits_mvs.service_date_end_date, NULL::date)          AS service_date_end_date,
           nvl2(CASE
                 WHEN (
                   merged_credits_mvs.first_billed_date < '1200-01-01 00:00:00'::timestamp without time zone
                 ) = true
                 THEN NULL::boolean
                 ELSE merged_credits_mvs.first_billed_date < '1200-01-01 00:00:00'::timestamp without time zone
                END, merged_credits_mvs.first_billed_date, NULL::timestamp without time zone)  AS first_billed_date,
           nvl2(CASE
                 WHEN (
                   merged_credits_mvs.scheduled_time < '1200-01-01 00:00:00'::timestamp without time zone
                 ) = true
                 THEN NULL::boolean
                 ELSE merged_credits_mvs.scheduled_time < '1200-01-01 00:00:00'::timestamp without time zone
                END, merged_credits_mvs.scheduled_time, NULL::timestamp without time zone) AS scheduled_time,
           merged_credits_mvs.institutional_claim_flag,
           merged_credits_mvs.primary_insurer_company,
           merged_credits_mvs.primary_insurer_payer_id,
           merged_credits_mvs.secondary_insurer_company,
           merged_credits_mvs.secondary_insurer_payer_id,
           CASE
              WHEN merged_credits_mvs.deposit_date < '1100-01-01'::date
              THEN NULL::date
              ELSE merged_credits_mvs.deposit_date
           END AS deposit_date,
           merged_credits_mvs.payer_id,
           merged_credits_mvs.insurance_name,
           CASE
              WHEN merged_credits_mvs.total_paid < ( - 450000.00::numeric(18,0) )
              THEN NULL::numeric::numeric(18,0)
              ELSE merged_credits_mvs.total_paid
           END AS total_paid,
           merged_credits_mvs.era_trace_number

          FROM (
                SELECT *
                FROM   {{ref('daysheet_credits_unmatched_era_mv')}}
                UNION ALL (
                    SELECT *
                    FROM   {{ref('daysheet_credits_matched_mv')}}
                )
                UNION ALL (
                    SELECT *
                    FROM   {{ref('daysheet_credits_unmatched_appt_mv')}}
                )
                UNION ALL (
                    SELECT *
                    FROM   {{ref('daysheet_credits_unmatched_pt_mv')}}
                )
                UNION ALL (
                    SELECT *
                    FROM   {{ref('daysheet_credits_unmatched_appt_pt_mv')}}
                )
      ) merged_credits_mvs
