{{ 
    config(
        SORT=["practice_group_id", "doctor_id"],
        materialized = "incremental",
        unique_key = 'line_item_transaction_id'
    ) 
}}

WITH fresh_data AS (
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
    and GREATEST(lit_created_at,lit_posted_date, era_deposit_date) > current_date - INTERVAL '365 days'
    and (era_is_verified or not pgo.verify_era_before_post)
)

{% if is_incremental() %}
,max_updated_at AS (
    SELECT
        max(li_updated_at) AS max_li_updated_at,
        max(appt_updated_at) AS max_appt_updated_at,
        max(doc_updated_at) AS max_doc_updated_at,
        max(office_updated_at) AS max_office_updated_at,
        max(patient_updated_at) AS max_patient_updated_at,
        max(lit_updated_at) AS max_lit_updated_at,
        max(era_updated_at) AS max_era_updated_at
    FROM {{ this }}
)

SELECT * FROM fresh_data
    WHERE (
        li_updated_at > max_li_updated_at
        OR appt_updated_at > max_appt_updated_at
        OR doc_updated_at > max_doc_updated_at
        OR office_updated_at > max_office_updated_at
        OR patient_updated_at > max_patient_updated_at
        OR lit_updated_at > max_lit_updated_at
        OR era_updated_at > max_era_updated_at
    )
{% else %}

SELECT * FROM fresh_data

{% endif %}

{{ apply_limit_if_test() }}
