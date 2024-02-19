{{ 
    config(
        SORT = ['practice_group_id', 'doctor_id']
    ) 
}}

with lineitems as ( select * FROM  {{ ref("int_lineitems") }} ),
cash_payments as ( select * FROM  {{ ref("stg_cash_payments") }} ),

final as (SELECT li.line_item_id
    --patient
    ,{{ patient_fields("li") }}
    ,li.patient_primary_insurance_company
    -- doctor
    ,{{ doctor_fields("li") }}
    ,{{ office_fields("li") }}
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
  , li.appt_first_billed_date 
  , li.appt_last_billed_date
  , li.appt_date_of_service

FROM lineitems AS li
LEFT JOIN cash_payments AS bcp
    ON bcp.line_item_id = li.line_item_id
WHERE (li.bli_balance_ins != 0 OR li.bli_balance_pt != 0)
)

SELECT * FROM final
WHERE practice_group_id = 1479