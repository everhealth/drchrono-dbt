SELECT *
FROM {{ source( 'chronometer_production', 'billing_billinglineitem' ) }}
WHERE created_at > '2023-01-01'