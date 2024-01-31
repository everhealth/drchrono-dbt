{{ config(MATERIALIZED='table', profile='scrubbed', schema='chronometer_scrubbed') }}

SELECT *
FROM {{ source( 'chronometer_production', 'chronometer_office' ) }}