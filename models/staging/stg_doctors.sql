select
    id as doctor_id,
    practice_group_id,
    verify_era_before_post,
    case when salutation is not null then salutation || ' ' else '' end
    || firstname
    || ' '
    || lastname
    || case
        when suffix is not null then ', ' || suffix else ''
    end as doc_fullname,
    updated_at as doc_updated_at

from {{ source("chronometer_production", "chronometer_doctor") }} d
WHERE {{ filter_pg("d")}} 
and _fivetran_deleted is false