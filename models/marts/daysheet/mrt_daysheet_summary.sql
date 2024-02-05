SELECT
    --filter fields
    'debit'             AS dtype
  , doctor_id
  , office_id
  , bli_code
  , practice_group_id
  , appt_scheduled_time AS dos
  , bli_created_at      AS debit_posted_date
  , NULL                AS ca_posted_date
  , NULL                AS ca_check_date
  , NULL                AS ca_deposit_date
  , NULL                AS cash_posted_date
  , NULL                AS cash_recieved_date

    --metric fields
  , bli_billed          AS debit_amount
  , NULL                AS credit_amount
  , NULL                AS adjustment_amount
  , NULL                AS patient_payment_amount

FROM public_marts.mrt_daysheet_debits

UNION

SELECT
    --filter fields
    'credit'         AS dtype
  , doctor_id
  , office_id
  , bli_code
  , practice_group_id

  , NULL             AS dos
  , NULL             AS debit_posted_date
  , lit_created_at   AS ca_posted_date
  , lit_posted_date  AS ca_check_date
  , era_deposit_date AS ca_deposit_date
  , NULL             AS cash_posted_date
  , NULL             AS cash_recieved_date

    --metric fields
  , NULL             AS debit_amount
  , lit_ins_paid     AS credit_amount
  , NULL             AS adjustment_amount
  , NULL             AS patient_payment_amount

FROM public_marts.mrt_daysheet_credits

UNION

SELECT
    --filter fields
    'ajdustment'     AS dtype
  , doctor_id
  , office_id
  , bli_code
  , practice_group_id

  , NULL             AS dos
  , NULL             AS debit_posted_date
  , lit_created_at   AS ca_posted_date
  , lit_posted_date  AS ca_check_date
  , era_deposit_date AS ca_deposit_date
  , NULL             AS cash_posted_date
  , NULL             AS cash_recieved_date

    --metric fields
  , NULL             AS debit_amount
  , NULL             AS credit_amount
  , lit_adjustment   AS adjustment_amount
  , NULL             AS patient_payment_amount
FROM public_marts.mrt_daysheet_adjustments

UNION

SELECT
    --filter fields
    'cash'        AS dtype
  , doctor_id
  , office_id
  , bli_code
  , practice_group_id

  , NULL          AS dos
  , NULL          AS debit_posted_date
  , NULL          AS ca_posted_date
  , NULL          AS ca_check_date
  , NULL          AS ca_deposit_date
  , posted_date   AS cash_posted_date
  , received_date AS cash_recieved_date

    --metric fields
  , NULL          AS debit_amount
  , NULL          AS credit_amount
  , NULL          AS adjustment_amount
  , amount        AS patient_payment_amount
FROM public_marts.mrt_daysheet_cashpayments