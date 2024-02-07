{{ config(SORT=["practice_group_id", "doctor_id"]) }}

select
    -- filter fields
    'debit' as daysheet_type,
    {{ doctor_fields("debits") }},
    {{ office_fields("debits") }},
    {{ patient_fields("debits") }},
    ins_info_name,
    ins_info_payer_id,
    exam_room_name,
    billing_code,
    practice_group_id,
    appt_date_of_service as dos,
    bli_created_at as debit_posted_date,
    null as ca_posted_date,
    null as ca_check_date,
    null as ca_deposit_date,
    null as cash_posted_date,
    null as cash_payment_date,

    -- metric fields
    bli_billed as debit_amount,
    null as credit_amount,
    null as adjustment_amount,
    null as patient_payment_amount

from {{ ref("mrt_daysheet_debits") }} as debits

union distinct

select
    -- filter fields
    'credit' as daysheet_type,
    {{ doctor_fields("credits") }},
    {{ office_fields("credits") }},
    {{ patient_fields("credits") }},
    ins_info_name,
    ins_info_payer_id,
    exam_room_name,
    billing_code,
    practice_group_id,
    null as dos,
    null as debit_posted_date,
    lit_created_at as ca_posted_date,
    lit_posted_date as ca_check_date,
    era_deposit_date as ca_deposit_date,
    null as cash_posted_date,
    null as cash_payment_date,

    -- metric fields
    null as debit_amount,
    lit_ins_paid as credit_amount,
    null as adjustment_amount,
    null as patient_payment_amount

from {{ ref("mrt_daysheet_credits") }} as credits

union distinct

select
    -- filter fields
    'adjustment' as daysheet_type,
    {{ doctor_fields("adjustments") }},
    {{ office_fields("adjustments") }},
    {{ patient_fields("adjustments") }},
    ins_info_name,
    ins_info_payer_id,
    exam_room_name,
    billing_code,
    practice_group_id,
    null as dos,
    null as debit_posted_date,
    lit_created_at as ca_posted_date,
    lit_posted_date as ca_check_date,
    era_deposit_date as ca_deposit_date,
    null as cash_posted_date,
    null as cash_payment_date,

    -- metric fields
    null as debit_amount,
    null as credit_amount,
    lit_adjustment as adjustment_amount,
    null as patient_payment_amount
from {{ ref("mrt_daysheet_adjustments") }} as adjustments

union distinct

select
    -- filter fields
    'cash' as daysheet_type,
    {{ doctor_fields("patient_payments") }},
    {{ office_fields("patient_payments") }},
    {{ patient_fields("patient_payments") }},
    ins_info_name,
    ins_info_payer_id,
    exam_room_name,
    billing_code,
    practice_group_id,
    null as dos,
    null as debit_posted_date,
    null as ca_posted_date,
    null as ca_check_date,
    null as ca_deposit_date,
    posted_date as cash_posted_date,
    payment_date as cash_payment_date,

    -- metric fields
    null as debit_amount,
    null as credit_amount,
    null as adjustment_amount,
    amount as patient_payment_amount
from {{ ref("mrt_daysheet_cashpayments") }} as patient_payments
