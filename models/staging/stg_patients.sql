SELECT
    id                        AS patient_id
  , chart_id                  AS patient_chart_id
  , patient_payment_profile
  , primary_insurance_company AS patient_primary_insurance_company
  , first_name                AS patient_first_name
  , middle_name               AS patient_middle_name
  , last_name                 AS patient_last_name
  , CASE
        WHEN middle_name = ''
            THEN first_name || ' ' || last_name
        WHEN middle_name IS NOT NULL
            THEN first_name || ' ' || middle_name || ' ' || last_name
    END                       AS patient_full_name
FROM {{source( 'chronometer_scrubbed', 'chronometer_patient' ) }}