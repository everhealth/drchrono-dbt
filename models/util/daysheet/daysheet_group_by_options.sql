{{ 
    config(
        post_hook="GRANT SELECT ON {{ this }} TO superset_user"
    ) 
}}

WITH group_choices AS (
    SELECT
        'Office' AS group_by,
        'Provider' AS sub_group_by,
        'Insurance' AS tertiary_group_by
    UNION ALL
    SELECT
        'Office',
        'Provider',
        'Procedure Code'
    UNION ALL
    SELECT
        'Office',
        'Insurance',
        'Provider'
    UNION ALL
    SELECT
        'Office',
        'Insurance',
        'Procedure Code'
    UNION ALL
    SELECT
        'Office',
        'Procedure Code',
        'Provider'
    UNION ALL
    SELECT
        'Office',
        'Procedure Code',
        'Insurance'
    UNION ALL
    SELECT
        'Provider',
        'Office',
        'Insurance'
    UNION ALL
    SELECT
        'Provider',
        'Office',
        'Procedure Code'
    UNION ALL
    SELECT
        'Provider',
        'Insurance',
        'Procedure Code'
    UNION ALL
    SELECT
        'Provider',
        'Insurance',
        'Office'
    UNION ALL
    SELECT
        'Provider',
        'Procedure Code',
        'Office'
    UNION ALL
    SELECT
        'Provider',
        'Procedure Code',
        'Insurance'
    UNION ALL
    SELECT
        'Insurance',
        'Office',
        'Provider'
    UNION ALL
    SELECT
        'Insurance',
        'Office',
        'Procedure Code'
    UNION ALL
    SELECT
        'Insurance',
        'Provider',
        'Procedure Code'
    UNION ALL
    SELECT
        'Insurance',
        'Provider',
        'Office'
    UNION ALL
    SELECT
        'Insurance',
        'Procedure Code',
        'Office'
    UNION ALL
    SELECT
        'Insurance',
        'Procedure Code',
        'Provider'
    UNION ALL
    SELECT
        'Procedure Code',
        'Office',
        'Provider'
    UNION ALL
    SELECT
        'Procedure Code',
        'Office',
        'Insurance'
    UNION ALL
    SELECT
        'Procedure Code',
        'Provider',
        'Office'
    UNION ALL
    SELECT
        'Procedure Code',
        'Provider',
        'Insurance'
    UNION ALL
    SELECT
        'Procedure Code',
        'Insurance',
        'Office'
    UNION ALL
    SELECT
        'Procedure Code',
        'Insurance',
        'Provider'
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
    gc.group_by,
    gc.sub_group_by,
    gc.tertiary_group_by
FROM
    doctor_groups AS dg
CROSS JOIN
    group_choices AS gc
ORDER BY
    dg.practice_group_id, dg.doctor_id
