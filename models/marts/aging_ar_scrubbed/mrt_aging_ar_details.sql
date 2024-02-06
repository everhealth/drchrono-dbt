{{ 
    config(
        SORT = ['practice_group_id', 'doctor_id', 'created_at', 'scheduled_time']
    ) 
}}

SELECT
    bli.id
  , {{doctor_fields("dd")}}
  , dd.practice_group_id
  , bli.balance_ins
  , bli.balance_pt
  , bli.billed
  , bli.allowed -- TODO: We may not need this column?
  , bli.price
  , bli.quantity
  , bli.adjustment
  , bli.expected_reimbursement -- for AR Days
  , bli.procedure_type
  , bli.code
  , bli.created_at
  , bli.from_date
  , bli.to_date
  , bli.appointment_id
  , CASE
        WHEN DATEDIFF( 'day', ca.first_billed_date, CURRENT_DATE ) BETWEEN 0 AND 30
            THEN 'a. 0-30 days'
        WHEN DATEDIFF( 'day', ca.first_billed_date, CURRENT_DATE ) BETWEEN 31 AND 60
            THEN 'b. 31-60 days'
        WHEN DATEDIFF( 'day', ca.first_billed_date, CURRENT_DATE ) BETWEEN 61 AND 90
            THEN 'c. 61-90 days'
        WHEN DATEDIFF( 'day', ca.first_billed_date, CURRENT_DATE ) BETWEEN 91 AND 120
            THEN 'd. 91-120 days'
            ELSE 'e. 121+ days'
    END                                           AS "30day_bucket"
  , DATE_TRUNC( 'month', ca.first_billed_date )   AS month_bucket
  , DATE_TRUNC( 'quarter', ca.first_billed_date ) AS quarter_bucket
  , ca.scheduled_time

FROM {{ source( 'chronometer_scrubbed', 'billing_billinglineitem' ) }} bli
LEFT JOIN {{ source('chronometer_scrubbed', 'chronometer_appointment') }} ca
ON ca.id = bli.appointment_id
    LEFT JOIN {{ source('chronometer_scrubbed', 'billing_cashpayment') }} bcp
    ON bcp.line_item_id = bli.id
    JOIN {{ ref('stg_doctors') }} dd
    ON ca.doctor_id = dd.doctor_id
