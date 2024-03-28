{{ 
    config(
        SORT=["practice_group_id", "doctor_id"],
        unique_key = 'line_item_transaction_id',
        post_hook="GRANT SELECT ON {{ this }} TO superset_user"
    ) 
}}

WITH final AS (
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
        ins_paid != 0
        AND lit_is_archived IS false
        AND (era_is_verified OR NOT pgo.verify_era_before_post)
)

SELECT * FROM final

{{ apply_limit_if_test() }}
