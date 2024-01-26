{{ config(materialized='table', dist="even") }}


SELECT *
	 , CASE
		   WHEN doc_verify_era_before_post IS TRUE AND era_is_verified IS FALSE
			   THEN 'exclude_era_payment'
		   WHEN doc_verify_era_before_post IS TRUE AND era_is_verified IS NULL
			   THEN 'exclude_era_payment'
			   ELSE 'include_era_payment'
	   END AS include_era_payment_status
FROM {{ ref('rcm_all_lineitem_transactions') }}
WHERE
		COALESCE( appointment_status, '' ) NOT IN ( 'No Show', 'Cancelled', 'Rescheduled' )
  AND   COALESCE( lit_adjusted_adjustment_reason, '' ) NOT IN ( 'PATIENT_RESPONSIBLE', 'SKIP_SECONDARY', 'DENIAL' )
  AND   COALESCE( lit_adjustment_reason, '' ) NOT IN ( '-3', '253', '225', '1', '2', '3' )
		-- adjustment_reasons: -3 = insurance payment, 253 = sequestration, 225 = interest, 1 = deductible, 2 = coinsurance, 3 = copayment
  AND   lit_ins_paid = 0
  AND   lit_is_archived IS FALSE
  AND   include_era_payment_status = 'include_era_payment'
