select
    id as line_item_transaction_id,
    line_item_id,
    era_id as lit_era_id,
    trace_number as lit_trace_number,
    claim_status as lit_claim_status,
    adjustment_group_code as lit_adjustment_group_code,
    allowed as lit_allowed,
    adjustment as lit_adjustment,
    ins_paid as lit_ins_paid,
    adjustment_reason as lit_adjustment_reason,
    adjusted_adjustment_reason as lit_adjusted_adjustment_reason,
    code as lit_code,
    status as lit_status,
    is_archived as lit_is_archived,
    ins_idx as lit_ins_idx,
    convert_timezone('EST', 'UTC', posted_date) as lit_posted_date,
    convert_timezone('EST', 'UTC', created_at) as lit_created_at
from {{ source("chronometer_production", "billing_lineitemtransaction") }}
