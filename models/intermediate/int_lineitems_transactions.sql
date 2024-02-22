with line_items as (select * from {{ ref("int_lineitems") }}),

era_objects as (select * from {{ ref("stg_era_objects") }}),

line_item_transactions as (select * from {{ ref("stg_line_item_transactions") }}
),

final as (
    select
        li.*,
        era.*,
        lit.*,
        case
            when ins_idx = 1 then li.primary_insurer_company
            when ins_idx = 2 then li.secondary_insurer_company
        end as ins_info_name,
        case
            when ins_idx = 1 then li.primary_insurer_payer_id
            when ins_idx = 2 then li.secondary_insurer_payer_id
        end as ins_info_payer_id
    from line_item_transactions as lit
    left join line_items as li using (line_item_id)
left join era_objects as era using (era_id)
where lit.line_item_id is NULL or li.line_item_id is not NULL
)

select * from final
