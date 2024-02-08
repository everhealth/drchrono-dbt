select
    id as doctor_id,
    practice_group_id,
    verify_era_before_post as doc_verify_era_before_post,
    case when salutation is not null then salutation || ' ' else '' end
    || firstname
    || ' '
    || lastname
    || case
        when suffix is not null then ', ' || suffix else ''
    end as doc_fullname

from {{ source("chronometer_scrubbed", "chronometer_doctor") }} d
WHERE {{ filter_pg("d")}}