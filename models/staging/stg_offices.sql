select
    id as office_id,
    name as office_name,
    state as office_state,
    facility_name as office_facility_name,
    facility_code as office_facility_code,
    exam_room_1_name,
    exam_room_2_name,
    exam_room_3_name,
    exam_room_4_name,
    exam_room_5_name,
    exam_room_6_name,
    exam_room_7_name,
    exam_room_8_name,
    exam_room_9_name,
    exam_room_10_name,
    exam_room_11_name,
    exam_room_12_name,
    exam_room_13_name,
    exam_room_14_name,
    exam_room_15_name,
    updated_at as office_updated_at
from {{ source("chronometer_production", "chronometer_office") }}
WHERE _fivetran_deleted is false