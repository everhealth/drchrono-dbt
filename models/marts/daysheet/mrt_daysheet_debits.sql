{{ config(SORT=["practice_group_id", "doctor_id"], materialized='incremental') }}

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
    and DATEDIFF(day, GREATEST(bli_created_at, appt_date_of_service), CURRENT_DATE) < 365
    {% if is_incremental() %}
        and bli_created_at > (select max(bli_created_at) from {{ this }})
    {% endif %}
{{ apply_limit_if_test() }}
