SELECT 
bcp.id,
bcp.appointment_id,
bcp.line_item_id,
bcp.posted_date,
bcp.received_date,
bcp.created_by_id,
bcp.notes,
bcp.amount,
bcp.trace_number,
bcp.code,
bcp.payment_method,
bcp.doctor_id,
bcp.patient_id,
bcp.parent_id

FROM {{source( 'chronometer_scrubbed', 'billing_cashpayment' ) }} bcp