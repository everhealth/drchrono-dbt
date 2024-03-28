{{ 
    config(
        SORT=["practice_group_id", "doctor_id"],
        unique_key = 'line_item_transaction_id',
        post_hook="GRANT SELECT ON {{ this }} TO superset_user"
    ) 
}}

WITH final AS (
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
    and (era_is_verified or not pgo.verify_era_before_post)
)

SELECT * FROM final

{{ apply_limit_if_test() }}
