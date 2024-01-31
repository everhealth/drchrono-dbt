SELECT *
FROM {{ source( 'chronometer_production', 'billing_lineitemtransaction' ) }}
WHERE created_at > '2023-01-01'