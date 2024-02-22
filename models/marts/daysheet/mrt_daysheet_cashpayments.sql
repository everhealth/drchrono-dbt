{{
    config(
        SORT=["doctor_id", "posted_date", "appt_date_of_service"],
        materialized='incremental',
        unique_key = 'cashpayment_id'
    ) 
}}

WITH fresh_data AS (

    SELECT

    -- CashPayment
        bcp.cashpayment_id,
        bcp.appointment_id,
        bcp.line_item_id,
        bcp.posted_date,
        bcp.payment_date,
        bcp.created_by_id,
        bcp.amount,
        bcp.trace_number,
        bcp.payment_method,
        bcp.parent_id,
    -- Patient
    {{ patient_fields("p") }},
    -- Doctor
    {{ doctor_fields() }},
    d.practice_group_id,
    -- office
    {{ office_fields("o") }},
    -- Appointment
    -- exam_room: ID and NAME
    a.examination_room as exam_room_id,
    {{ exam_room_name("a", "o") }},
    a.service_date_start_date,
    a.service_date_end_date,
    a.first_billed_date,
    a.date_of_service,
    a.institutional_claim_flag,
    bli.code as billing_code,
    primary_insurer_company as ins_info_name,
    primary_insurer_payer_id as ins_info_payer_id,        
    li_updated_at ,
    appt_updated_at ,
    doc_updated_at ,
    office_updated_at,
    patient_updated_at ,
    cash_updated_at
    FROM {{ ref("stg_cash_payments") }} AS bcp
    LEFT JOIN
        {{ ref("stg_appointments") }} AS a
        ON bcp.appointment_id = a.appointment_id
    LEFT JOIN
        {{ ref("stg_doctors") }} AS d
        ON coalesce(a.doctor_id, bcp.doctor_id) = d.doctor_id
    LEFT JOIN
        {{ ref("stg_patients") }} AS p
        ON bcp.patient_id = p.patient_id
    LEFT JOIN {{ ref("stg_offices") }} AS o ON a.office_id = o.office_id
    LEFT JOIN
        {{ ref("stg_line_items") }} AS bli
        ON bcp.line_item_id = bli.line_item_id

    WHERE
        bcp.amount != 0
        AND coalesce(a.appointment_status, '') NOT IN (
            'Cancelled', 'Rescheduled'
        )
        AND datediff(
            DAY, greatest(bcp.posted_date, bcp.payment_date), current_date
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
            MAX(patient_updated_at) AS max_patient_updated_at    
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
