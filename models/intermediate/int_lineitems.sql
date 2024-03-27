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
    line_items AS (SELECT * FROM {{ ref("stg_line_items") }})

    , appointment_info AS (SELECT * FROM {{ ref("int_appointments") }})

    , final AS (
        SELECT

            ai.*
            , li.*
            
        FROM line_items AS li
            LEFT JOIN
                appointment_info AS ai ON ai.appointment_id = li.li_appointment_id
        WHERE li.li_appointment_id IS NULL OR ai.appointment_id IS NOT NULL
    )

SELECT * FROM final
