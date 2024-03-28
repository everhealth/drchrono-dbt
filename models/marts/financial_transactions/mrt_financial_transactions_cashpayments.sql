{{
    config(
        SORT=["doctor_id", "posted_date", "appt_date_of_service"],
        unique_key = 'cashpayment_id',
        post_hook="GRANT SELECT ON {{ this }} TO superset_user"
    ) 
}}

WITH final AS (

    SELECT

    -- CashPayment
    cashpayment_id,
    appointment_id,
    practice_group_id,
    posted_date,
    payment_date,
    cashpayment_amount,
    trace_number,
    payment_method,
    -- Patient
    {{ patient_fields("cp") }},
    {{ doctor_fields("cp") }},
    {{ office_fields("cp") }},
    exam_room_name,
    date_of_service,
    billing_code,
    primary_insurer_company AS ins_info_name,
    primary_insurer_payer_id AS ins_info_payer_id,   
    li_updated_at ,
    appt_updated_at ,
    doc_updated_at ,
    office_updated_at,
    patient_updated_at ,
    cash_updated_at
    FROM {{ ref("int_cashpayments") }} as cp

    WHERE
        cashpayment_amount != 0
)

SELECT * FROM final

{{ apply_limit_if_test() }}
