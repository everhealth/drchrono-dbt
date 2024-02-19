WITH bucket_type_choices AS (
    SELECT '30 days' AS bucket_type

    UNION DISTINCT
    SELECT 'Month'

    UNION DISTINCT
    SELECT 'Quarter'
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
    bt.bucket_type
FROM
    doctor_groups AS dg
CROSS JOIN
    bucket_type_choices AS bt
ORDER BY
    dg.practice_group_id,
    dg.doctor_id,
    bt.bucket_type
