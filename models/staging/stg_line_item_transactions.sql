select
    id as line_item_transaction_id,
    line_item_id,
    era_id,
    trace_number,
    claim_status,
    adjustment_group_code,
    allowed,
    adjustment,
    ins_paid,
    adjustment_reason,
    adjusted_adjustment_reason,
    code,
    status as lit_status,
    is_archived as lit_is_archived,
    ins_idx,
    updated_at as lit_updated_at,
    convert_timezone('EST', 'UTC', posted_date) as lit_posted_date,
    convert_timezone('EST', 'UTC', created_at) as lit_created_at
from {{ source("chronometer_production", "billing_lineitemtransaction") }}
where _fivetran_deleted is false
