{{ 
    config(
        SORT=["practice_group_id", "doctor_id"],
        materialized = "incremental",
        unique_key='line_item_transaction_id',
        incremental_strategy='delete+insert',

    ) 
}}

select
    lit.*
from {{ ref("int_lineitems_transactions") }} lit
inner join
    {{ ref("stg_practice_group_options") }} as pgo
    on lit.practice_group_id = pgo.practice_group_id
where
    coalesce(appointment_status, '') not in (
        'No Show', 'Cancelled', 'Rescheduled'
    )
    and coalesce(lit_adjusted_adjustment_reason, '')
    not in ('PATIENT_RESPONSIBLE', 'SKIP_SECONDARY', 'DENIAL')
    and coalesce(lit_adjustment_reason, '') not in (
        '-3', '253', '225', '1', '2', '3'
    )
    -- adjustment_reasons: -3 = insurance payment, 253 = sequestration, 225 =
    -- interest, 1 = deductible, 2 = coinsurance, 3 = copayment
    and lit_ins_paid = 0
    and lit_is_archived is false
    and datediff(day, GREATEST(lit_created_at,lit_posted_date, era_deposit_date) , current_date) < 365
    and (era_is_verified or not pgo.verify_era_before_post)
    {% if is_incremental() %}
        and lit_created_at > (select max(lit_created_at) from {{ this }})
    {% endif %}
{{ apply_limit_if_test() }}

