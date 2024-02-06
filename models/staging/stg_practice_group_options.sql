SELECT
    practice_group_id,
    verify_era_before_post
FROM {{ source("chronometer_production", "chronometer_practicegroupoptions") }}
