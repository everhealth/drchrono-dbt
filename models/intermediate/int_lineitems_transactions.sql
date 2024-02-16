select
    ali.*,
    era.*,
    lit.line_item_transaction_id,
    lit.lit_trace_number,
    lit.lit_claim_status,
    lit.lit_adjustment_group_code,
    lit.lit_posted_date,
    lit.lit_allowed,
    lit.lit_adjustment,
    lit.lit_ins_paid,
    lit.lit_adjustment_reason,
    lit.lit_created_at,
    lit.lit_adjusted_adjustment_reason,
    lit.lit_code,
    lit.lit_status,
    lit.lit_is_archived,
    lit.lit_ins_idx,
    case
        when lit_ins_idx = 1 then ali.appt_primary_insurer_company
        when lit_ins_idx = 2 then ali.appt_secondary_insurer_company
    end as ins_info_name,
    case
        when lit_ins_idx = 1 then ali.appt_primary_insurer_payer_id
        when lit_ins_idx = 2 then ali.appt_secondary_insurer_payer_id
    end as ins_info_payer_id,
    lit.lit_updated_at
from {{ ref("stg_line_item_transactions") }} as lit
left join
    {{ ref("int_lineitems") }} as ali
    on lit.line_item_id = ali.line_item_id
left join {{ ref("stg_era_objects") }} as era on lit.lit_era_id = era.era_id
where lit.line_item_id is NULL or ali.line_item_id is not NULL
