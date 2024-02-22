SELECT
    id            AS patient_id
    , chart_id
    , patient_payment_profile
    , primary_insurance_company
    , secondary_insurance_company
    , primary_insurance_plan_type
    , secondary_insurance_plan_type
    , first_name  AS patient_first_name
    , middle_name AS patient_middle_name
    , last_name   AS patient_last_name
    , CASE
        WHEN middle_name = ''
            THEN first_name || ' ' || last_name
        WHEN middle_name IS NOT NULL
            THEN first_name || ' ' || middle_name || ' ' || last_name
    END           AS patient_fullname
    , updated_at  AS patient_updated_at
FROM {{ source("chronometer_production", "chronometer_patient") }}
WHERE
    is_demo_data_patient IS FALSE
    AND _fivetran_deleted IS FALSE
