{{ 
    config(
        SORT=["practice_group_id", "doctor_id"],
        materialized="table"
    )
}}
WITH

, line_item_transactions AS (SELECT * FROM {{ ref("stg_line_item_transactions") }}

SELECT *,
((ins1_status = '' AND ins2_status = '') OR
       (ins1_status = 'ERA Denied' AND ca.billing_status = 'Bill Secondary Insurance' AND ins2_status = '')) as claim_rebilled 
FROM line_item_transactions
WHERE  denied_flag = TRUE
