

SELECT *
	 , CASE
		   WHEN doc_verify_era_before_post IS TRUE AND era_is_verified IS FALSE
			   THEN 'exclude_era_payment'
		   WHEN doc_verify_era_before_post IS TRUE AND era_is_verified IS NULL
			   THEN 'exclude_era_payment'
			   ELSE 'include_era_payment'
	   END AS include_era_payment_status
FROM {{ ref('int_lineitems_transactions') }}
WHERE
		COALESCE( appointment_status, '' ) NOT IN ( 'No Show', 'Cancelled', 'Rescheduled' )
  AND   lit_ins_paid <> 0
  AND   lit_adjustment_reason IN ( '-3', '253', '225' )
		-- adjustment_reasons: -3 = insurance payment, 225 = interest, 253 = sequestration
  AND   lit_is_archived IS FALSE
  AND   include_era_payment_status = 'include_era_payment'
  AND   DATEDIFF( DAY, bli_created_at, CURRENT_DATE )
