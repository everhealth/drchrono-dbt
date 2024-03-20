{{ 
    config(
        post_hook="GRANT SELECT ON {{ this }} TO superset_user"
    ) 
}}

WITH group_by_choices AS (
    SELECT 'Provider' AS group_by

    UNION DISTINCT
    SELECT 'Office'

    UNION DISTINCT
    SELECT 'Billing Status'    
    
    UNION DISTINCT
    SELECT 'Claim Status'  

    UNION DISTINCT
    SELECT 'Insurance'    

    UNION DISTINCT
    SELECT 'Insurance Plan Type'
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
    gb.group_by
FROM
    doctor_groups AS dg
CROSS JOIN
    group_by_choices AS gb
ORDER BY
    dg.practice_group_id,
    dg.doctor_id,
    gb.group_by
