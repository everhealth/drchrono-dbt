{{
    config(
        SORT=[
            "bli_created_at",
            "date_of_service",
            "practice_group_id",
            "doctor_id",
        ]
    )
}}


WITH

    appointment_info AS (SELECT * FROM {{ ref("int_appointments") }})

    , cash_payments AS (SELECT * FROM {{ ref("stg_cash_payments") }})

    , line_items AS (SELECT * FROM {{ ref("stg_line_items") }})

    , final AS (

        SELECT

            cp.*
            , ai.*
            , li.*
        FROM cash_payments AS cp
            LEFT JOIN appointment_info AS ai ON cp.cash_appointment_id = ai.appointment_id
            LEFT JOIN line_items AS li ON cp.cash_line_item_id = li.line_item_id
    )

SELECT * FROM final
