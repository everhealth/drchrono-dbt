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
cash_payments AS (SELECT * FROM {{ ref("stg_cash_payments") }})

, appointments AS (SELECT * FROM {{ ref("stg_appointments") }})

, doctors AS (SELECT * FROM {{ ref("stg_doctors") }})

, patients AS (SELECT * FROM {{ ref("stg_patients") }})

, offices AS (SELECT * FROM {{ ref("stg_offices") }})

, line_items AS (SELECT * FROM {{ ref("stg_line_items") }})

, final AS (

    SELECT

        cp.*
        , a.*
        , d.*
        , p.*
        , o.*
        , li.*
        , {{ exam_room_name("a","o") }}
    FROM cash_payments AS cp
        LEFT JOIN appointments AS a ON cp.cash_appointment_id = a.appointment_id
        LEFT JOIN doctors AS d ON COALESCE(a.appt_doctor_id, cp.cash_doctor_id) = d.doctor_id
        LEFT JOIN patients AS p ON cp.cash_patient_id = p.patient_id
        LEFT JOIN offices AS o ON a.appt_office_id = o.office_id
        LEFT JOIN line_items AS li ON cp.cash_line_item_id = li.line_item_id
)

SELECT * FROM final
