{{ 
    config(
        SORT = ['practice_group_id', 'doctor_id']
    ) 
}}

WITH lineitems AS (SELECT * FROM {{ ref("int_lineitems") }})

, cash_payments AS (SELECT * FROM {{ ref("stg_cash_payments") }})

, final AS (
    SELECT
        li.line_item_id
        --patient
        , {{ patient_fields("li") }}
        , li.primary_insurance_company
        -- doctor
        , {{ doctor_fields("li") }}
        , {{ office_fields("li") }}
        , li.practice_group_id
        , li.balance_ins
        , li.balance_pt
        , li.billed
        , li.li_allowed
        , li.price
        , li.quantity
        , li.li_adjustment
        , li.expected_reimbursement
        , li.procedure_type
        , li.billing_code
        , li.li_created_at
        , li.first_billed_date
        , li.last_billed_date
        , li.date_of_service
        , li.appointment_id
        , li.appt_billing_status

    FROM lineitems AS li
        LEFT JOIN cash_payments AS bcp
            ON li.line_item_id = bcp.cash_line_item_id
    WHERE (li.balance_ins != 0 OR li.balance_pt != 0)
)

SELECT * FROM final

{{ apply_limit_if_test() }}
