SELECT
    id           AS doctor_id
    , practice_group_id
    , verify_era_before_post
    , CASE WHEN salutation IS NOT NULL THEN salutation || ' ' ELSE '' END
    || firstname
    || ' '
    || lastname
    || CASE
        WHEN suffix IS NOT NULL THEN ', ' || suffix ELSE ''
    END          AS doc_fullname
    , updated_at AS doc_updated_at

FROM {{ source("chronometer_production", "chronometer_doctor") }} AS d
WHERE         {{ filter_pg("d") }} 
    AND _fivetran_deleted IS FALSE
