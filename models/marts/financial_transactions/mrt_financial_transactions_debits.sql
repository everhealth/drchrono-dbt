{{ 
    config(
        SORT=["practice_group_id", "doctor_id"],
        unique_key = 'line_item_id',
        post_hook="GRANT SELECT ON {{ this }} TO superset_user"
    ) 
}}

WITH final AS (
    SELECT
        line_item_id,
        appointment_id, 
        {{ patient_fields("li") }}, 
        {{ office_fields("li") }}, 
        {{ doctor_fields("li") }}, 
        exam_room_name,
        billing_code,
        practice_group_id,
        primary_insurer_company AS ins_info_name,
        primary_insurer_payer_id AS ins_info_payer_id,
        date_of_service, 
        li_created_at,
        billed,            
        li_updated_at,
        appt_updated_at,
        doc_updated_at,
        office_updated_at,
        patient_updated_at
 

    FROM {{ ref("int_lineitems") }} li
    WHERE
        NOT (
            COALESCE(appointment_status, '') = 'No Show'
            AND procedure_type IN ('C', 'H', 'R')
        )
        AND COALESCE(appointment_status, '') NOT IN ('Cancelled', 'Rescheduled')
        AND billed > 0
)

SELECT * FROM final

{{ apply_limit_if_test() }}
