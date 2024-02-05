select
    -- filter fields
    'debit' as daysheet_type,
    {{ doctor_fields("d") }},
    {{ office_fields("d") }},
    bli_code,
    practice_group_id,
    appt_scheduled_time as dos,
    bli_created_at as debit_posted_date,
    null as ca_posted_date,
    null as ca_check_date,
    null as ca_deposit_date,
    null as cash_posted_date,
    null as cash_recieved_date,

    -- metric fields
    bli_billed as debit_amount,
    null as credit_amount,
    null as adjustment_amount,
    null as patient_payment_amount

from {{ ref("mrt_daysheet_debits") }}

union distinct

select
    -- filter fields
    'credit' as daysheet_type,
    {{ doctor_fields("c") }},
    {{ office_fields("c") }},
    bli_code,
    practice_group_id,
    null as dos,
    null as debit_posted_date,
    lit_created_at as ca_posted_date,
    lit_posted_date as ca_check_date,
    era_deposit_date as ca_deposit_date,
    null as cash_posted_date,
    null as cash_recieved_date,

    -- metric fields
    null as debit_amount,
    lit_ins_paid as credit_amount,
    null as adjustment_amount,
    null as patient_payment_amount

from {{ ref("mrt_daysheet_credits") }}

union distinct

select
    -- filter fields
    'ajdustment' as daysheet_type,
    {{ doctor_fields("a") }},
    {{ office_fields("a") }},
    bli_code,
    practice_group_id,
    null as dos,
    null as debit_posted_date,
    lit_created_at as ca_posted_date,
    lit_posted_date as ca_check_date,
    era_deposit_date as ca_deposit_date,
    null as cash_posted_date,
    null as cash_recieved_date,

    -- metric fields
    null as debit_amount,
    null as credit_amount,
    lit_adjustment as adjustment_amount,
    null as patient_payment_amount
from {{ ref("mrt_daysheet_adjustments") }}

union distinct

select
    -- filter fields
    'cash' as daysheet_type,
    {{ doctor_fields("p") }},
    {{ office_fields("p") }},
    bli_code,
    practice_group_id,
    null as dos,
    null as debit_posted_date,
    null as ca_posted_date,
    null as ca_check_date,
    null as ca_deposit_date,
    posted_date as cash_posted_date,
    received_date as cash_recieved_date,

    -- metric fields
    null as debit_amount,
    null as credit_amount,
    null as adjustment_amount,
    amount as patient_payment_amount
from {{ ref("mrt_daysheet_cashpayments") }}
