{{ 
    config(
        SORT = ['practice_group_id', 'doctor_id']
    ) 
}}

SELECT li.line_item_id
    --patient
    ,{{ patient_fields("li") }}
    ,li.patient_primary_insurance_company
    -- doctor
    ,{{ doctor_fields("li") }}
   ,li.practice_group_id
  , li.bli_balance_ins
  , li.bli_balance_pt
  , li.bli_billed
  , li.bli_allowed 
  , li.bli_price
  , li.bli_quantity
  , li.bli_adjustment
  , li.bli_expected_reimbursement 
  , li.bli_procedure_type
  , li.billing_code
  , li.bli_created_at
  , li.appointment_id
  , li.appt_first_billed_date 
  , li.appt_last_billed_date
  , li.appt_date_of_service
--We can move this logic to a custom field in superset, which will also allow us to make the dates interchangagble. 
--     , CASE
--         WHEN DATEDIFF( 'day', ca.first_billed_date, CURRENT_DATE ) BETWEEN 0 AND 30
--             THEN 'a. 0-30 days'
--         WHEN DATEDIFF( 'day', ca.first_billed_date, CURRENT_DATE ) BETWEEN 31 AND 60
--             THEN 'b. 31-60 days'
--         WHEN DATEDIFF( 'day', ca.first_billed_date, CURRENT_DATE ) BETWEEN 61 AND 90
--             THEN 'c. 61-90 days'
--         WHEN DATEDIFF( 'day', ca.first_billed_date, CURRENT_DATE ) BETWEEN 91 AND 120
--             THEN 'd. 91-120 days'
--             ELSE 'e. 121+ days'
--     END                                           AS "30day_bucket"
--   , DATE_TRUNC( 'month', ca.first_billed_date )   AS month_bucket
--   , DATE_TRUNC( 'quarter', ca.first_billed_date ) AS quarter_bucket
FROM {{ ref("int_lineitems") }} AS li
LEFT JOIN {{ ref("stg_cash_payments") }} AS bcp
    ON bcp.line_item_id = li.line_item_id
WHERE (li.bli_balance_ins != 0 OR li.bli_balance_pt != 0)
