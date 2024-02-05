{{ config(
    SORT = ['doctor_id', 'posted_date', 'scheduled_time']
) }}


SELECT
  -- CashPayment
  bcp.*
  -- Patient
  , {
    {
    patient_fields(
    'p'
    )}}
  -- Doctor
  , {
    {
    doctor_fields(
    'd'
    )}}
  , cd.practice_group_id
--office
  , {
    {
    office_fields(
    'o'
    )}}
  -- Appointment
  -- exam_room: ID and NAME
    a.examination_room AS exam_room_id
  , {
    {
    exam_room_name(
    )}}
  , a.service_date_start_date
  , a.service_date_end_date
  , a.first_billed_date
  , a.scheduled_time
  , a.institutional_claim_flag
  , bli.code AS billing_code
FROM
    {{ ref('stg_cash_payments') }} bcp
LEFT JOIN {{ ref('stg_appointments') }} a
    ON bcp.appointment_id = a.appointment_id
LEFT JOIN {{ ref('stg_doctors') }} d
    ON a.doctor_id = d.doctor_id
LEFT JOIN {{ ref('stg_patients') }} p
    ON bcp.patient_id = p.patient_id
LEFT JOIN {{ ref('stg_offices') }} o
    ON a.office_id = o.office_id
LEFT JOIN {{ ref( 'stg_line_items' ) }} bli
    ON bcp.line_item_id = bli.line_item_id

WHERE
    amount <> 0
  AND COALESCE (
    a.appointment_status
    , '') NOT IN (
    'Cancelled'
    , 'Rescheduled')
