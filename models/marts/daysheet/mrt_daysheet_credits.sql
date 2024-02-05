select
    *,
    case
        when doc_verify_era_before_post is true and era_is_verified is false
            then 'exclude_era_payment'
        when doc_verify_era_before_post is true and era_is_verified is null
            then 'exclude_era_payment'
        else 'include_era_payment'
    end as include_era_payment_status
from {{ ref("int_lineitems_transactions") }}
where
    coalesce(appointment_status, '') not in (
        'No Show', 'Cancelled', 'Rescheduled'
    )
    and lit_ins_paid != 0
    and lit_adjustment_reason in ('-3', '253', '225')
    -- adjustment_reasons: -3 = insurance payment, 225 = interest, 253 = sequestration
    and lit_is_archived is false
    and include_era_payment_status = 'include_era_payment'
    and datediff(day, lit_created_at, current_date) < 365
