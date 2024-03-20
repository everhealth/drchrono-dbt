{{ 
    config(
        SORT = ['practice_group_id', 'doctor_id'],
        post_hook="GRANT SELECT ON {{ this }} TO superset_user"
    ) 
}}

WITH lineitems AS (SELECT * FROM {{ ref("int_lineitems") }}),

aggs as (
    SELECT line_item_id,
    sum(balance_ins) as total_claim_bal_ins,
    sum(balance_pt) as total_claim_bal_pt,
    count(expected_reimbursement) as total_claim_expeceted
    FROM lineitems GROUP BY 1
)

, final AS (
    SELECT
        aggs.*
        --patient
        , {{ patient_fields("li") }}
        , patient_date_of_birth
        , primary_insurance_company
        -- doctor
        , {{ doctor_fields("li") }}
        , {{ office_fields("li") }}
        , practice_group_id
        , balance_ins
        , balance_pt
        , billed
        , li_allowed
        , price
        , quantity
        , li_adjustment
        , expected_reimbursement
        , procedure_type
        , billing_code
        , li_created_at
        , first_billed_date
        , last_billed_date
        , date_of_service
        , appointment_id
        , appt_billing_status
        , icd_codes_str
        , modifiers_json
        , diagnosis_pointers_json

    FROM lineitems li
    LEFT JOIN aggs on li.line_item_id = aggs.line_item_id
    WHERE (balance_ins != 0 OR balance_pt != 0)
)

SELECT * FROM final

{{ apply_limit_if_test() }}
