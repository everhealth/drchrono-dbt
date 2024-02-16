{{ 
    config(
        SORT=["practice_group_id", "doctor_id"],
        materialized = "incremental",
        unique_key = 'line_item_id'
    ) 
}}

WITH fresh_data AS (
    SELECT
        *,
        appt_primary_insurer_company AS ins_info_name,
        appt_primary_insurer_payer_id AS ins_info_payer_id
    FROM {{ ref("int_lineitems") }}
    WHERE
        NOT (
            COALESCE(appointment_status, '') = 'No Show'
            AND bli_procedure_type IN ('C', 'H', 'R')
        )
        AND COALESCE(appointment_status, '') NOT IN ('Cancelled', 'Rescheduled')
        AND bli_billed > 0
        AND DATEDIFF(
            DAY, GREATEST(bli_created_at, appt_date_of_service), CURRENT_DATE
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
        > SELECT minmax FROM min_of_max
    )
{% else %}

SELECT * FROM fresh_data

{% endif %}
{{ apply_limit_if_test() }}