

SELECT
	ali.*
  , lit.*
  , era.*
FROM {{ ref('stg_line_item_transactions') }} lit
LEFT JOIN {{ ref('int_lineitems') }} ali USING (line_item_id)
LEFT JOIN {{ ref('stg_era_objects') }} era USING (era_id)
WHERE
	 lit.line_item_id IS NULL
  OR ali.line_item_id IS NOT NULL
