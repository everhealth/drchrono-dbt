{{ config(MATERIALIZED='table', profile='scrubbed') }}

SELECT *
FROM {{ source( 'chronometer_production', 'chronometer_office' ) }}