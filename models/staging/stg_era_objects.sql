select
    id as era_id,
    payer_id as era_payer_id,
    insurance_name as era_insurance_name,
    total_paid as era_total_paid,
    is_verified as era_is_verified,
    is_eob as era_is_eob,
    is_archived as era_is_archived,
    convert_timezone('EST', 'UTC', posted_date) as era_posted_date,
    convert_timezone('EST', 'UTC', created_at) as era_created_at,
    convert_timezone('EST', 'UTC', updated_at) as era_updated_at,
    convert_timezone('EST', 'UTC', deposit_date) as era_deposit_date
from {{ source("chronometer_production", "billing_eraobject") }}
