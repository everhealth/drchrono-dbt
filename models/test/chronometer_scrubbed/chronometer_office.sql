SELECT *
FROM {{ source( 'chronometer_production', 'chronometer_office' ) }}