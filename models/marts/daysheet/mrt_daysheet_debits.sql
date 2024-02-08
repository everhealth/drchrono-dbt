{{ config(SORT=["practice_group_id", "doctor_id"]) }}

select
    *,
    appt_primary_insurer_company as ins_info_name,
    appt_primary_insurer_payer_id as ins_info_payer_id
from {{ ref("int_lineitems") }}
where
    not (
        COALESCE(appointment_status, '') = 'No Show'
        and bli_procedure_type in ('C', 'H', 'R')
    )
    and COALESCE(appointment_status, '') not in ('Cancelled', 'Rescheduled')
    and bli_billed > 0
    and DATEDIFF(day, bli_created_at, CURRENT_DATE) < 365
{{ apply_limit_if_test() }}
