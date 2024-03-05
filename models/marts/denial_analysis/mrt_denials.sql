{{ 
    config(
        SORT=["practice_group_id", "doctor_id"],
        materialized="table"
    )
}}
WITH

    line_item_transactions AS (SELECT * FROM {{ ref("int_lineitems_transactions") }})

    , line_items AS (SELECT * FROM {{ ref("int_lineitems") }}) 

    , all_codes AS (
        SELECT
            l1.appointment_id
            , LISTAGG(DISTINCT(l2.billing_code), ', ') AS all_billed_codes
        FROM line_items AS l1
            INNER JOIN line_items AS l2
                ON l1.appointment_id = l2.appointment_id
                WHERE l1.appointment_id IS NOT NULL
                GROUP BY 1
    )

SELECT

    lit.appointment_id
    , {{ patient_fields("lit") }}
    , {{ doctor_fields("lit") }}
    , {{ office_fields("lit") }}
    , exam_room_name
    , billing_provider_id
    , billing_provider_name
    , date_of_service
    , lit_created_at    AS posted_date
    , lit_posted_date   AS check_date
    , billing_code
    , adjustment_reason
    , lit_adjustment AS denial_amount
    , ins_info_name
    , ins_info_payer_id
    , lit_claim_status
    , icd_codes_str
    , all_billed_codes
    , institutional_claim_flag
    , (
        balance_ins = 0 AND balance_pt = 0
    )                   AS is_zero_balance
    , (
        (ins1_status = '' AND ins2_status = '')
        OR (ins1_status = 'ERA Denied' AND appt_billing_status = 'Bill Secondary Insurance' AND ins2_status = '')
    )                   AS claim_rebilled
FROM line_item_transactions AS lit
    INNER JOIN all_codes ON lit.appointment_id = all_codes.appointment_id
WHERE denied_flag = TRUE

{{ apply_limit_if_test() }}
