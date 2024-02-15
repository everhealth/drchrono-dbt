{{ 
    config(
        SORT=["practice_group_id", "doctor_id"],
        materialized = "incremental",
        unique_key = 'line_item_transaction_id'
    ) 
}}

WITH fresh_data AS (
    SELECT lit.*
    FROM {{ ref("int_lineitems_transactions") }} AS lit
    INNER JOIN
        {{ ref("stg_practice_group_options") }} AS pgo
        ON lit.practice_group_id = pgo.practice_group_id
    WHERE
        coalesce(appointment_status, '') NOT IN (
            'No Show', 'Cancelled', 'Rescheduled'
        )
        AND lit_ins_paid != 0
        AND lit_adjustment_reason IN ('-3', '253', '225')
        -- adjustment_reasons: -3 = insurance payment, 225 = interest, 253 = sequestration
        AND lit_is_archived IS false
        AND greatest(lit_created_at, lit_posted_date, era_deposit_date)
        > current_date - INTERVAL '365 days'
        AND (era_is_verified OR NOT pgo.verify_era_before_post)
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
