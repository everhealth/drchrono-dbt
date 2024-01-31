{{ config(MATERIALIZED='table', profile='scrubbed', schema='chronometer_scrubbed') }}

SELECT *
FROM {{ source( 'chronometer_production', 'billing_lineitemtransaction' ) }}