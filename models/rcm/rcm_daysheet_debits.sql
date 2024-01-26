{{ config(materialized='table', dist="even") }}

SELECT *
FROM {{ ref('rcm_all_lineitems') }}
WHERE
  coalesce(appointment_status,'') NOT IN ('No Show','Cancelled','Rescheduled')
  AND bli_billed > 0