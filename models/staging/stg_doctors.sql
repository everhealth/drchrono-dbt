SELECT
    id                                         AS doctor_id
  , practice_group_id
  , verify_era_before_post                     AS doc_verify_era_before_post
  , CASE
        WHEN salutation IS NOT NULL
            THEN salutation || ' '
            ELSE ''
    END || firstname || ' ' || lastname || CASE
                                               WHEN suffix IS NOT NULL
                                                   THEN ', ' || suffix
                                                   ELSE ''
                                           END AS doc_fullname

FROM {{ source( 'chronometer_scrubbed', 'chronometer_doctor' ) }}