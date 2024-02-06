select *
from {{ ref("int_lineitems") }}
where
    NOT (
        COALESCE(appointment_status, '') = 'No Show'
        AND procedure_type IN ('C', 'H', 'R')
    )
    AND COALESCE(appointment_status, '') NOT IN ('Cancelled', 'Rescheduled')
    AND billed > 0
    AND DATEDIFF(day, created_at, CURRENT_DATE) < 365
{{ apply_limit_if_test() }}