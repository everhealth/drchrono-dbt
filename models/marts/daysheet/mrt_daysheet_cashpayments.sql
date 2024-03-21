{{
    config(
        SORT=["doctor_id", "posted_date", "appt_date_of_service"],
        materialized='incremental',
        unique_key = 'cashpayment_id',
        post_hook="GRANT SELECT ON {{ this }} TO superset_user"
    ) 
}}

WITH fresh_data AS (

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
        AND datediff(
            DAY, greatest(posted_date, payment_date), current_date
        )
        < 365
)

{% if is_incremental() %}
    , max_updated_at AS (
        SELECT
            MAX(li_updated_at) AS max_li_updated_at,
            MAX(appt_updated_at) AS max_appt_updated_at,
            MAX(doc_updated_at) AS max_doc_updated_at,
            MAX(office_updated_at) AS max_office_updated_at,
            MAX(patient_updated_at) AS max_patient_updated_at,    
            MAX(cash_updated_at) AS max_cash_updated_at     
        FROM {{ this }}
    ),

    min_of_max AS (
        SELECT
            LEAST(
                max_li_updated_at,
                max_appt_updated_at,
                max_doc_updated_at,
                max_office_updated_at,
                max_patient_updated_at,
                max_cash_updated_at
            ) AS minmax
        FROM max_updated_at
    )

    SELECT * FROM fresh_data
    WHERE (
        GREATEST(
            li_updated_at,
            appt_updated_at,
            doc_updated_at,
            office_updated_at,
            patient_updated_at,
            cash_updated_at
        )
        > (SELECT minmax FROM min_of_max)
    )
{% else %}
SELECT * FROM fresh_data

{% endif %}
{{ apply_limit_if_test() }}
