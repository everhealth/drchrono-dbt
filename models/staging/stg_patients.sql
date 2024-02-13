select
    id as patient_id,
    chart_id as patient_chart_id,
    patient_payment_profile,
    primary_insurance_company as patient_primary_insurance_company,
    first_name as patient_first_name,
    middle_name as patient_middle_name,
    last_name as patient_last_name,
    case
        when middle_name = ''
            then first_name || ' ' || last_name
        when middle_name is not null
            then first_name || ' ' || middle_name || ' ' || last_name
    end as patient_fullname
from {{ source("chronometer_production", "chronometer_patient") }}
WHERE is_demo_data_patient is false
