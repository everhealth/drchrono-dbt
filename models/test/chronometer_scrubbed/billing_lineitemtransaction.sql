SELECT *
FROM {{ source( 'chronometer_production', 'billing_lineitemtransaction' ) }}