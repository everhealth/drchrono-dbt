SELECT
    id            AS office_id
  , name          AS office_name
  , state         AS office_state
  , facility_name AS office_facility_name
  , facility_code AS office_facility_code
FROM {{source( 'chronometer_scrubbed', 'chronometer_office' ) }}