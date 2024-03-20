{{ 
    config(
        post_hook="GRANT SELECT ON {{ this }} TO superset_user"
    ) 
}}

WITH date_type_choices AS (
    SELECT 'First Billed Date' AS date_type

    UNION DISTINCT
    SELECT 'Last Billed Date'

    UNION DISTINCT
    SELECT 'Date of Service'
),

doctor_groups AS (
    SELECT DISTINCT
        practice_group_id,
        doctor_id
    FROM {{ ref("stg_doctors") }}
)

SELECT
    dg.practice_group_id,
    dg.doctor_id,
    dt.date_type
FROM
    doctor_groups AS dg
CROSS JOIN
    date_type_choices AS dt
ORDER BY
    dg.practice_group_id,
    dg.doctor_id,
    dt.date_type
