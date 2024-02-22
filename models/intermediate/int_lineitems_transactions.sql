WITH
line_items AS (SELECT * FROM {{ ref("int_lineitems") }})

, era_objects AS (SELECT * FROM {{ ref("stg_era_objects") }})

, line_item_transactions AS (SELECT * FROM {{ ref("stg_line_item_transactions") }}
)

, final AS (
    SELECT
        li.*
        , era.*
        , lit.*
        , CASE
            WHEN ins_idx = 1
                THEN li.primary_insurer_company
            WHEN ins_idx = 2
                THEN li.secondary_insurer_company
        END AS ins_info_name
        , CASE
            WHEN ins_idx = 1
                THEN li.primary_insurer_payer_id
            WHEN ins_idx = 2
                THEN li.secondary_insurer_payer_id
        END AS ins_info_payer_id
    FROM line_item_transactions AS lit
        LEFT JOIN line_items AS li ON lit.lit_line_item_id = li.line_item_id
        LEFT JOIN era_objects AS era ON lit.lit_era_id = era.era_id
    WHERE lit.lit_line_item_id IS NULL OR li.line_item_id IS NOT NULL
)

SELECT *
FROM final
