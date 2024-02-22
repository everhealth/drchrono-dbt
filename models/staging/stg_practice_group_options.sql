SELECT
    practice_group_id
    , verify_era_before_post
    , updated_at AS pgo_updated_at
FROM {{ source("chronometer_production", "chronometer_practicegroupoptions") }}
WHERE _fivetran_deleted IS FALSE
