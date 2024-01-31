{{ config(MATERIALIZED='table', profile='scrubbed', SCHEMA='chronometer_scrubbed') }}

SELECT
    id
  , primary_patient_relationship_to_subscriber
  , primary_insurance_group_number
  , primary_insurance_plan_type
  , follow_up_appointment_reason
  , initial_treatment_date
  , created_at
  , primary_care_physician
  , date_of_last_appointment
  , statement_balance
  , CONCAT( LEFT( first_name, 1 ), '*****' )                   AS first_name
  , CONCAT( LEFT( last_name, 1 ), '*****' )                    AS last_name
  , primary_insurance_company
  , current_balance
  , is_demo_data_patient
  , date_of_first_appointment
  , patient_payment_profile
  , suffix
  , copay
  , DATEADD( DAY, CEIL( rand( ) * 30000 )::INT, '1940-01-01' ) AS date_of_birth
  , updated_at
  , LEFT( primary_insurance_id_number, 4 )                     AS primary_insurance_id_number
  , chart_id
  , primary_insurance_notes
  , statement_balance_timestamp
  , insurance_clearing_house
  , secondary_insurance_plan_name
  , primary_insurance_payer_id
  , previous_name
  , statement_due_date
  , patient_status
  , doctor_id
  , secondary_insurance_company
  , secondary_insurance_payer_id
  , onpatient_id
  , primary_insurance_plan_name
  , title
  , _fivetran_deleted
  , _fivetran_synced
FROM {{ source( 'chronometer_production', 'chronometer_patient' ) }}