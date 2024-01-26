{{ 
    config(
        materialized='table', 
        dist="even", 
        schema = 'public_scrubbed',
        sort = ['doctor_id']
    ) 
}}

SELECT
    ca.doctor_id
  , dd.firstname || ' ' || dd.lastname AS doctor_name
  , (CASE
        WHEN DATEDIFF('day', ca.first_billed_date, CURRENT_DATE) BETWEEN 0 AND 30 THEN 'a. 0-30 days'
        WHEN DATEDIFF('day', ca.first_billed_date, CURRENT_DATE) BETWEEN 31 AND 60 THEN 'b. 31-60 days'
        WHEN DATEDIFF('day', ca.first_billed_date, CURRENT_DATE) BETWEEN 61 AND 90 THEN 'c. 61-90 days'
        WHEN DATEDIFF('day', ca.first_billed_date, CURRENT_DATE) BETWEEN 91 AND 120 THEN 'd. 91-120 days'
        ELSE 'e. 121+ days'
    END)  AS ar_bucket
  , NULLIF( SUM( bli.balance_pt * 1 ), NULL )  AS patient_ar
  , NULLIF( SUM( bli.balance_ins * 1 ), NULL ) AS insurance_ar
FROM {{ source('chronometer_scrubbed','billing_billinglineitem') }} bli
LEFT JOIN {{ source('chronometer_scrubbed','chronometer_appointment') }} ca
     ON ca.id = bli.appointment_id
LEFT JOIN {{ source('chronometer_scrubbed','billing_cashpayment') }} bcp
     ON bcp.line_item_id = bli.id
JOIN {{ source('chronometer_scrubbed','chronometer_doctor') }} dd
     ON ca.doctor_id = dd.id
GROUP BY 1, 2, 3


