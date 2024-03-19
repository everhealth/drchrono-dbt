{{ 
    config(
        SORT=["practice_group_id", "doctor_id"],
        materialized = "incremental",
        unique_key = 'line_item_transaction_id',
        post_hook="GRANT SELECT ON {{ this }} TO superset_user"
    ) 
}}

WITH fresh_data AS (
    select
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
        lit_adjustment,
        li_updated_at,
        appt_updated_at,
        doc_updated_at,
        office_updated_at,
        patient_updated_at,
        lit_updated_at,
        era_updated_at,
        adjustment_reason,
        adjusted_adjustment_reason
from {{ ref("int_lineitems_transactions") }} lit
inner join
    {{ ref("stg_practice_group_options") }} as pgo
    on lit.practice_group_id = pgo.practice_group_id
where
    adjusted_adjustment_reason in ('ADJUST_INS', 'ADJUST_PT')
    and not (COALESCE(adjustment_group_code, '') = 'CO' AND ins_idx = 2)
    and lit_is_archived is false
    and GREATEST(lit_created_at,lit_posted_date, era_deposit_date) > current_date - INTERVAL '365 days'
    and (era_is_verified or not pgo.verify_era_before_post)
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
