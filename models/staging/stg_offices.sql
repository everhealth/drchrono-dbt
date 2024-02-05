select
    id as office_id,
    name as office_name,
    state as office_state,
    facility_name as office_facility_name,
    facility_code as office_facility_code
from {{ source("chronometer_scrubbed", "chronometer_office") }}
