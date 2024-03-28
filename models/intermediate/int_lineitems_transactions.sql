WITH

    appointment_info AS (SELECT * FROM {{ ref("int_appointments") }})

    , era_objects AS (SELECT * FROM {{ ref("stg_era_objects") }})

    , line_item_transactions AS (SELECT * FROM {{ ref("stg_line_item_transactions") }})

    , line_items AS (SELECT * FROM {{ ref("stg_line_items") }})

    , final AS (
        SELECT
            li.*
            , ai.*
            , era.*
            , lit.*
            , CASE
                WHEN ins_idx = 1
                    THEN ai.primary_insurer_company
                WHEN ins_idx = 2
                    THEN ai.secondary_insurer_company
            END AS ins_info_name
            , CASE
                WHEN ins_idx = 1
                    THEN ai.primary_insurer_payer_id
                WHEN ins_idx = 2
                    THEN ai.secondary_insurer_payer_id
            END AS ins_info_payer_id
        FROM line_item_transactions AS lit
            LEFT JOIN era_objects AS era ON lit.lit_era_id = era.era_id
            LEFT JOIN appointment_info AS ai ON lit.lit_appointment_id = ai.appointment_id
            LEFT JOIN line_items AS li ON lit.lit_line_item_id = li.line_item_id
        WHERE lit.lit_line_item_id IS NULL OR li.line_item_id IS NOT NULL
    )

SELECT *
FROM final
