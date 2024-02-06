select *
from {{ ref("int_lineitems") }}
where
    coalesce(appointment_status, '') not in (
        'No Show', 'Cancelled', 'Rescheduled'
    )
    and bli_billed > 0
    and datediff(day, bli_created_at, current_date) < 365
