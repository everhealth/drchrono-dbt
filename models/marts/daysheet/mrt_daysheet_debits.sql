{{ 
    config(
        SORT=["practice_group_id", "doctor_id"],
        materialized = "incremental",
        unique_key = 'line_item_id'
    ) 
}}

WITH fresh_data AS (
    SELECT
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
        AND DATEDIFF(
            DAY, GREATEST(li_created_at, date_of_service), CURRENT_DATE
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
        FROM {{ this }}
    ),

    min_of_max AS (
        SELECT
            LEAST(
                max_li_updated_at,
                max_appt_updated_at,
                max_doc_updated_at,
                max_office_updated_at,
                max_patient_updated_at
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
            patient_updated_at
        )
        > (SELECT minmax FROM min_of_max)
    )
{% else %}

SELECT * FROM fresh_data

{% endif %}
{{ apply_limit_if_test() }}
