{{ 
    config(
        SORT=["practice_group_id", "doctor_id"],
        materialized = "view",
    ) 
}}

SELECT
    -- filter fields
    'debit'         AS daysheet_type
    , {{ doctor_fields("debits") }}
    , {{ office_fields("debits") }}
    , {{ patient_fields("debits") }}
    , ins_info_name
    , ins_info_payer_id
    , exam_room_name
    , billing_code
    , practice_group_id

    --dates
    , date_of_service
    , li_created_at AS debit_posted_date
    , NULL          AS ca_posted_date
    , NULL          AS ca_check_date
    , NULL          AS ca_deposit_date
    , NULL          AS cash_posted_date
    , NULL          AS cash_payment_date

    -- metric fields
    , billed        AS debit_amount
    , NULL          AS credit_amount
    , NULL          AS adjustment_amount
    , NULL          AS patient_payment_amount

FROM {{ ref("mrt_daysheet_debits") }} AS debits

UNION DISTINCT

SELECT
    -- filter fields
    'credit'           AS daysheet_type
    , {{ doctor_fields("credits") }}
    , {{ office_fields("credits") }}
    , {{ patient_fields("credits") }}
    , ins_info_name
    , ins_info_payer_id
    , exam_room_name
    , billing_code
    , practice_group_id

    --dates
    , NULL             AS date_of_service
    , NULL             AS debit_posted_date
    , lit_created_at   AS ca_posted_date
    , lit_posted_date  AS ca_check_date
    , era_deposit_date AS ca_deposit_date
    , NULL             AS cash_posted_date
    , NULL             AS cash_payment_date

    -- metric fields
    , NULL             AS debit_amount
    , ins_paid         AS credit_amount
    , NULL             AS adjustment_amount
    , NULL             AS patient_payment_amount

FROM {{ ref("mrt_daysheet_credits") }} AS credits

UNION DISTINCT

SELECT
    -- filter fields
    'adjustment'       AS daysheet_type
    , {{ doctor_fields("adjustments") }}
    , {{ office_fields("adjustments") }}
    , {{ patient_fields("adjustments") }}
    , ins_info_name
    , ins_info_payer_id
    , exam_room_name
    , billing_code
    , practice_group_id

    --dates
    , NULL             AS date_of_service
    , NULL             AS debit_posted_date
    , lit_created_at   AS ca_posted_date
    , lit_posted_date  AS ca_check_date
    , era_deposit_date AS ca_deposit_date
    , NULL             AS cash_posted_date
    , NULL             AS cash_payment_date

    -- metric fields
    , NULL             AS debit_amount
    , NULL             AS credit_amount
    , lit_adjustment   AS adjustment_amount
    , NULL             AS patient_payment_amount
FROM {{ ref("mrt_daysheet_adjustments") }} AS adjustments

UNION DISTINCT

SELECT
    -- filter fields
    'cash'               AS daysheet_type
    , {{ doctor_fields("patient_payments") }}
    , {{ office_fields("patient_payments") }}
    , {{ patient_fields("patient_payments") }}
    , ins_info_name
    , ins_info_payer_id
    , exam_room_name
    , billing_code
    , practice_group_id

    --dates
    , NULL               AS date_of_service
    , NULL               AS debit_posted_date
    , NULL               AS ca_posted_date
    , NULL               AS ca_check_date
    , NULL               AS ca_deposit_date
    , posted_date        AS cash_posted_date
    , payment_date       AS cash_payment_date

    -- metric fields
    , NULL               AS debit_amount
    , NULL               AS credit_amount
    , NULL               AS adjustment_amount
    , cashpayment_amount AS patient_payment_amount
FROM {{ ref("mrt_daysheet_cashpayments") }} AS patient_payments
