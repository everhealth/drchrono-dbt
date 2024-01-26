{{ config(materialized='table', dist="even") }}


SELECT
	ali.*
  , lit.id                                             AS line_item_transaction_id
  , lit.era_id                                         AS lit_era_id
  , lit.trace_number                                   AS lit_trace_number
  , lit.claim_status                                   AS lit_claim_status
  , lit.service_qualifier                              AS lit_service_qualifier
  , lit.adjustment_group_code                          AS lit_adjustment_group_code
  , CONVERT_TIMEZONE( 'EST', 'UTC', lit.posted_date )  AS lit_posted_date
  , lit.allowed                                        AS lit_allowed
  , lit.adjustment                                     AS lit_adjustment
  , lit.ins_paid                                       AS lit_ins_paid
  , lit.adjustment_reason                              AS lit_adjustment_reason
  , CONVERT_TIMEZONE( 'EST', 'UTC', lit.created_at )   AS lit_created_at
  , lit.adjusted_adjustment_reason                     AS lit_adjusted_adjustment_reason
  , lit.code                                           AS lit_code
  , lit.status                                         AS lit_status
  , lit.modifiers_json                                 AS lit_modifiers_json
  , lit.is_archived                                    AS lit_is_archived
  , lit.ins_idx                                        AS lit_ins_idx
	--era object
  , era.payer_id                                       AS era_payer_id
  , era.insurance_name                                 AS era_insurance_name
  , era.total_paid                                     AS era_total_paid
  , era.is_verified                                    AS era_is_verified
  , era.is_eob                                         AS era_is_eob
  , era.is_archived                                    AS era_is_archived
  , CONVERT_TIMEZONE( 'EST', 'UTC', era.posted_date )  AS era_posted_date
  , CONVERT_TIMEZONE( 'EST', 'UTC', era.created_at )   AS era_created_at
  , CONVERT_TIMEZONE( 'EST', 'UTC', era.updated_at )   AS era_updated_at
  , CONVERT_TIMEZONE( 'EST', 'UTC', era.deposit_date ) AS era_deposit_date
FROM {{source( 'chronometer_production', 'billing_lineitemtransaction' ) }} lit
LEFT JOIN {{ ref('rcm_all_lineitems') }} ali
		  ON ali.line_item_id = lit.line_item_id
LEFT JOIN {{source( 'chronometer_production', 'billing_eraobject' ) }} era
		  ON era.id = lit.era_id
WHERE
	 lit.line_item_id IS NULL
  OR ali.line_item_id IS NOT NULL
