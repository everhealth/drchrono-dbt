SELECT
    id                                            AS line_item_transaction_id
  , line_item_id
  , era_id                                        AS lit_era_id
  , trace_number                                  AS lit_trace_number
  , claim_status                                  AS lit_claim_status
  , adjustment_group_code                         AS lit_adjustment_group_code
  , CONVERT_TIMEZONE( 'EST', 'UTC', posted_date ) AS lit_posted_date
  , allowed                                       AS lit_allowed
  , adjustment                                    AS lit_adjustment
  , ins_paid                                      AS lit_ins_paid
  , adjustment_reason                             AS lit_adjustment_reason
  , CONVERT_TIMEZONE( 'EST', 'UTC', created_at )  AS lit_created_at
  , adjusted_adjustment_reason                    AS lit_adjusted_adjustment_reason
  , code                                          AS lit_code
  , status                                        AS lit_status
  , is_archived                                   AS lit_is_archived
  , ins_idx                                       AS lit_ins_idx
FROM {{source( 'chronometer_scrubbed', 'billing_lineitemtransaction' ) }}