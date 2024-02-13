WITH date_type_choices AS (
    SELECT
        'Posted Date' AS debits_date_type,
        'Posted Date' AS credits_date_type,
        'Posted Date' AS cash_date_type
    UNION DISTINCT
    SELECT
        'Date of Service',
        'Check Date',
        'Payment Date'
    UNION DISTINCT
    SELECT
        NULL,
        'Deposit Date',
        NULL
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
    dt.debits_date_type,
    dt.credits_date_type,
    dt.cash_date_type
FROM
    doctor_groups AS dg
CROSS JOIN
    date_type_choices AS dt
ORDER BY
    dg.practice_group_id, dg.doctor_id, dt.debits_date_type, dt.credits_date_type
