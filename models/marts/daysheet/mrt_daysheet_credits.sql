{{ 
    config(
        SORT=["practice_group_id", "doctor_id"],
        materialized = "incremental",
        unique_key = 'line_item_transaction_id',
        post_hook="GRANT SELECT ON {{ this }} TO superset_user"
    ) 
}}

WITH fresh_data AS (
    SELECT
        line_item_transaction_id,         
        appointment_id, 
        {{ patient_fields("lit") }}, 
        {{ office_fields("lit") }}, 
        {{ doctor_fields("lit") }},        
        exam_room_name,
        billing_code,
        lit.practice_group_id,
        ins_info_name,
        ins_info_payer_id,
        lit_created_at,
        lit_posted_date,
        era_deposit_date,
        date_of_service, 
        ins_paid,
        li_updated_at,
        appt_updated_at,
        doc_updated_at,
        office_updated_at,
        patient_updated_at,
        lit_updated_at,
        era_updated_at
    FROM {{ ref("int_lineitems_transactions") }} AS lit
    INNER JOIN
        {{ ref("stg_practice_group_options") }} AS pgo
        ON lit.practice_group_id = pgo.practice_group_id
    WHERE
        coalesce(appointment_status, '') NOT IN (
            'No Show', 'Cancelled', 'Rescheduled'
        )
        AND ins_paid != 0
        AND adjustment_reason IN ('-3', '253', '225')
        -- adjustment_reasons: -3 = insurance payment, 225 = interest, 253 = sequestration
        AND lit_is_archived IS false
        AND greatest(lit_created_at, lit_posted_date, era_deposit_date)
        > current_date - INTERVAL '365 days'
        AND (era_is_verified OR NOT pgo.verify_era_before_post)
)
{% if is_incremental() %}
    , max_updated_at AS (
        SELECT
            MAX(li_updated_at) AS max_li_updated_at,
            MAX(appt_updated_at) AS max_appt_updated_at,
            MAX(doc_updated_at) AS max_doc_updated_at,
            MAX(office_updated_at) AS max_office_updated_at,
            MAX(patient_updated_at) AS max_patient_updated_at,        
            max(lit_updated_at) AS max_lit_updated_at,
            max(era_updated_at) AS max_era_updated_at
        FROM {{ this }}
    ),

    min_of_max AS (
        SELECT
            LEAST(
                max_li_updated_at,
                max_appt_updated_at,
                max_doc_updated_at,
                max_office_updated_at,
                max_patient_updated_at,
                max_lit_updated_at,
                max_era_updated_at
            ) AS minmax
        FROM max_updated_at
    )

    SELECT * FROM fresh_data
    WHERE (
        GREATEST(
            li_updated_at,
            appt_updated_at,
            doc_updated_at,
            office_updated_at,
            patient_updated_at,
            lit_updated_at,
            era_updated_at
        )
        > (SELECT minmax FROM min_of_max)
    )
{% else %}
SELECT * FROM fresh_data

{% endif %}
{{ apply_limit_if_test() }}
