select lit.*
from {{ ref("int_lineitems_transactions") }} as lit
inner join
    {{ ref("stg_practice_group_options") }} as pgo
    on lit.practice_group_id = pgo.practice_group_id
where
    coalesce(appointment_status, '') not in (
        'No Show', 'Cancelled', 'Rescheduled'
    )
    and lit_ins_paid != 0
    and lit_adjustment_reason in ('-3', '253', '225')
    -- adjustment_reasons: -3 = insurance payment, 225 = interest, 253 = sequestration
    and lit_is_archived is false
    and datediff(day, lit_created_at, current_date) < 365
    and (era_is_verified or not pgo.verify_era_before_post)
{{ apply_limit_if_test() }}