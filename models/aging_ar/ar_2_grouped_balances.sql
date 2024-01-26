{{ config(
    materialized='view',
    dist='appointment_id',
    sort=['appointment_id', 'doctor_id', 'practice_group_id', 'is_ar_type_insurance']
) }}

SELECT
    ca.appointment_id,
    ca.doctor_id,
    doc.practice_group_id,
    SUM(bli.balance_ins) AS balance_ins_sum,
    SUM(bli.balance_pt) AS balance_pt_sum,
    SUM(bli.balance_ins + bli.balance_pt) AS balance_sum,
    (SUM(bli.balance_ins + bli.balance_pt) < 0) AS is_negative_balance,
    (SUM(bli.balance_ins + bli.balance_pt) > 0) AS is_positive_balance,
    (SUM(bli.balance_pt) != 0) AS is_ar_type_patient,
    (SUM(bli.balance_ins) != 0) AS is_ar_type_insurance,
    (
        SUM(bli.balance_pt) != 0
        AND SUM(bli.balance_ins) != 0
    ) AS is_ar_type_all
FROM
    chronometer_production.billing_billinglineitem bli
    JOIN chronometer_production.chronometer_appointment ca ON (
        bli.appointment_id = ca.id
        AND ca.appointment_status NOT IN ('Cancelled', 'Rescheduled')
        AND ca.appt_is_break = False
        AND ca.deleted_flag = False
        AND NOT (
            ca.is_special_case_of_recur_series = False
            AND ca.recurring_appointment = True
        )
        AND NOT (
            ca.appointment_status = 'No Show'
            AND bli.procedure_type IN ('C', 'H', 'R')
        )
        AND ca.scheduled_time <= CURRENT_DATE
    )
    JOIN chronometer_production.chronometer_doctor doc ON (ca.doctor_id = doc.id)
GROUP BY
    ca.appointment_id,
    ca.doctor_id,
    doc.practice_group_id
HAVING
    SUM(bli.balance_ins + bli.balance_pt) != 0
    AND (
        SUM(bli.balance_ins) != 0
        OR SUM(bli.balance_pt) != 0
    );
