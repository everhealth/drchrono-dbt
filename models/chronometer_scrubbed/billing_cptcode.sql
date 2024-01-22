{{ config(MATERIALIZED='table', profile='scrubbed') }}

SELECT *
FROM {{ source( 'chronometer_production', 'billing_cptcode' ) }}