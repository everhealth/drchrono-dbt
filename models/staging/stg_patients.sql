select
    id as patient_id,
    chart_id,
    patient_payment_profile,
    primary_insurance_company,
    secondary_insurance_company,
    primary_insurance_plan_type,
    secondary_insurance_plan_type,
    first_name as patient_first_name,
    middle_name as patient_middle_name,
    last_name as patient_last_name,
    case
        when middle_name = ''
            then first_name || ' ' || last_name
        when middle_name is not null
            then first_name || ' ' || middle_name || ' ' || last_name
    end as patient_fullname,
    updated_at as patient_updated_at
from {{ source("chronometer_production", "chronometer_patient") }}
WHERE is_demo_data_patient is false
and _fivetran_deleted is false