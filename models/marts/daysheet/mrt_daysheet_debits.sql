{{ 
    config(
        SORT=["practice_group_id", "doctor_id"],
        materialized = "incremental",
        unique_key = 'line_item_id'
    ) 
}}

WITH fresh_data AS (
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
)

{% if is_incremental() %}
,max_updated_at AS (
    SELECT
        max(li_updated_at) AS max_li_updated_at,
        max(appt_updated_at) AS max_appt_updated_at,
        max(doc_updated_at) AS max_doc_updated_at,
        max(office_updated_at) AS max_office_updated_at,
        max(patient_updated_at) AS max_patient_updated_at
    FROM {{ this }}
)

SELECT * FROM fresh_data
    WHERE (
        li_updated_at > max_li_updated_at
        OR appt_updated_at > max_appt_updated_at
        OR doc_updated_at > max_doc_updated_at
        OR office_updated_at > max_office_updated_at
        OR patient_updated_at > max_patient_updated_at
    )
{% else %}

SELECT * FROM fresh_data

{% endif %}
{{ apply_limit_if_test() }}
