{{ config(MATERIALIZED='table', dist="even", schema='public_scrubbed',
    sort = ['practice_group_id', 'doctor_id','created_at', 'scheduled_time']) }}

SELECT
    blit.*
  , dd.firstname || ' ' || dd.lastname  AS doctor_name
  , dd.practice_group_id
  , (CASE
         WHEN DATEDIFF('day', ca.first_billed_date, CURRENT_DATE) <= 30
             THEN 'a. 0-30 days'
         WHEN DATEDIFF('day', ca.first_billed_date, CURRENT_DATE) <= 60
             THEN 'b. 31-60 days'
         WHEN DATEDIFF('day', ca.first_billed_date, CURRENT_DATE) <= 90
             THEN 'c. 61-90 days'
         WHEN DATEDIFF('day', ca.first_billed_date, CURRENT_DATE) <= 120
             THEN 'd. 91-120 days'
             ELSE 'e. 121+ days'
     END)                                         AS "30day_bucket"
  , DATE_TRUNC('month', ca.first_billed_date )   AS month_bucket
  , DATE_TRUNC('quarter', ca.first_billed_date ) AS quarter_bucket

FROM {{ source( 'chronometer_scrubbed', 'billing_billinglineitem' ) }} bli
LEFT JOIN {{ source('chronometer_scrubbed','chronometer_appointment') }} ca
ON ca.id = bli.appointment_id
    LEFT JOIN {{ source('chronometer_scrubbed','billing_lineitemtransaction') }} blit
    ON blit.line_item_id = bli.id
    LEFT JOIN {{ source('chronometer_scrubbed','billing_cashpayment') }} bcp
    ON bcp.line_item_id = bli.id
    JOIN {{ source('chronometer_scrubbed','chronometer_doctor') }} dd
    ON blit.doctor_id = dd.id


