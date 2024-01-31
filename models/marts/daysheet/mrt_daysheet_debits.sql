SELECT *
FROM {{ ref( 'int_lineitems' ) }}
WHERE
        COALESCE( appointment_status, '' ) NOT IN ( 'No Show', 'Cancelled', 'Rescheduled' )
  AND   bli_billed > 0
  AND   DATEDIFF( DAY, bli_created_at, CURRENT_DATE ) < 365