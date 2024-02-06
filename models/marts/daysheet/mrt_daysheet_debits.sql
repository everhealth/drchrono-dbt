select *
from {{ ref("int_lineitems") }}
where
    NOT (
        COALESCE(appointment_status, '') = 'No Show'
        AND bli_procedure_type IN ('C', 'H', 'R')
    )
    AND COALESCE(appointment_status, '') NOT IN ('Cancelled', 'Rescheduled')
    AND bli_billed > 0
    AND DATEDIFF(day, bli_created_at, CURRENT_DATE) < 365
{{ apply_limit_if_test() }}