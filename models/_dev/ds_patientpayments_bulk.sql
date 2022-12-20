{{ config(
    materialized = 'view',
    sort = ['doctor_id', 'posted_date', 'scheduled_time'],
    auto_refresh = 'true'
) }}

{% set bcp_fields = [ "id", "posted_date", "received_date", "created_by_id", "notes", "amount", "trace_number", "payment_method", "doctor_id", "patient_id","parent_id" ] %}
{% set patient_fields = ["first_name", "middle_name", "last_name", "chart_id"] %}
{% set doctor_fields = ["firstname", "lastname", "salutation", "suffix", "practice_group_id"]%}
{% set appt_fields = ["office_id", "service_date_start_date", "service_date_end_date", "first_billed_date", "scheduled_time", "institutional_claim_flag"]%}

SELECT
    -- CashPayment
    {% for bcp_f in bcp_fields -%}
    bcp.{{bcp_f}},
    {% endfor -%}
    -- Patient
    {% for pt_f in patient_fields -%}
    cp.{{pt_f}},
    {% endfor -%}
    co.name,
    -- Doctor
    {% for doc_f in doctor_fields -%}
    cd.{{doc_f}},
    {% endfor -%}
    -- Appointment
    -- exam_room: ID and NAME
    bcp.appointment_id,
    ca.examination_room AS exam_room_id,
    DECODE(
        ca.examination_room,
        1,
        co.exam_room_1_name,
        2,
        co.exam_room_2_name,
        3,
        co.exam_room_3_name,
        4,
        co.exam_room_4_name,
        5,
        co.exam_room_5_name,
        6,
        co.exam_room_6_name,
        7,
        co.exam_room_7_name,
        8,
        co.exam_room_8_name,
        9,
        co.exam_room_9_name,
        10,
        co.exam_room_10_name,
        11,
        co.exam_room_11_name,
        12,
        co.exam_room_12_name,
        13,
        co.exam_room_13_name,
        14,
        co.exam_room_14_name,
        15,
        co.exam_room_15_name,
        ''
    ) AS exam_room_name,
    {%- for appt_f in appt_fields -%}
    ca.{{appt_f}},
    {% endfor -%}
    bli.code
FROM
    chronometer_production.billing_cashpayment bcp
    JOIN chronometer_production.chronometer_appointment ca
    ON (
        bcp.appointment_id = ca.id
        AND ca.deleted_flag IS FALSE
        AND ca.is_demo_data_appointment IS FALSE
        AND ca.appt_is_break IS FALSE
        AND COALESCE(
            ca.appointment_status,
            ''
        ) NOT IN (
            'No Show',
            'Cancelled',
            'Rescheduled'
        )
    )
    JOIN chronometer_production.chronometer_doctor cd
    ON (
        ca.doctor_id = cd.id
    )
    JOIN chronometer_production.chronometer_patient cp
    ON (
        bcp.patient_id = cp.id
    )
    JOIN chronometer_production.chronometer_office co
    ON (
        ca.office_id = co.id
    )
    JOIN chronometer_production.billing_billinglineitem bli
    ON (
        bcp.line_item_id = bli.id
    )
WHERE
    amount <> 0;
CREATE materialized VIEW PUBLIC.daysheet_patientpayments_allocated_not_bli_mv sortkey(
        doctor_id,
        posted_date,
        scheduled_time
    ) auto refresh yes AS
SELECT
    -- CashPayment
    bcp.id,
    bcp.posted_date,
    bcp.received_date,
    -- TODO
    bcp.created_by_id,
    bcp.notes,
    bcp.amount,
    bcp.trace_number,
    bcp.payment_method,
    bcp.doctor_id,
    -- Patient
    bcp.patient_id,
    cp.first_name,
    cp.middle_name,
    cp.last_name,
    -- Office
    cp.chart_id,
    -- Doctor
    co.name,
    cd.firstname,
    cd.lastname,
    cd.salutation,
    cd.suffix,
    -- Appointment
    cd.practice_group_id,
    -- exam_room: ID and NAME
    bcp.appointment_id,
    ca.examination_room AS exam_room_id,
    DECODE(
        ca.examination_room,
        1,
        co.exam_room_1_name,
        2,
        co.exam_room_2_name,
        3,
        co.exam_room_3_name,
        4,
        co.exam_room_4_name,
        5,
        co.exam_room_5_name,
        6,
        co.exam_room_6_name,
        7,
        co.exam_room_7_name,
        8,
        co.exam_room_8_name,
        9,
        co.exam_room_9_name,
        10,
        co.exam_room_10_name,
        11,
        co.exam_room_11_name,
        12,
        co.exam_room_12_name,
        13,
        co.exam_room_13_name,
        14,
        co.exam_room_14_name,
        15,
        co.exam_room_15_name,
        ''
    ) AS exam_room_name,
    ca.office_id,
    ca.service_date_start_date,
    ca.service_date_end_date,
    ca.first_billed_date,
    ca.scheduled_time,
    ca.institutional_claim_flag,
    bcp.line_item_id AS code
FROM
    chronometer_production.billing_cashpayment bcp
    JOIN chronometer_production.chronometer_appointment ca
    ON (
        bcp.appointment_id = ca.id
        AND ca.deleted_flag IS FALSE
        AND ca.is_demo_data_appointment IS FALSE
        AND ca.appt_is_break IS FALSE
        AND COALESCE(
            ca.appointment_status,
            ''
        ) NOT IN (
            'No Show',
            'Cancelled',
            'Rescheduled'
        )
    )
    JOIN chronometer_production.chronometer_doctor cd
    ON (
        ca.doctor_id = cd.id
    )
    JOIN chronometer_production.chronometer_patient cp
    ON (
        bcp.patient_id = cp.id
    )
    JOIN chronometer_production.chronometer_office co
    ON (
        ca.office_id = co.id
    )
WHERE
    amount <> 0
    AND bcp.line_item_id IS NULL;
-- Unallocated
    CREATE materialized VIEW PUBLIC.daysheet_patientpayments_unallocated_bli_mv sortkey(
        doctor_id,
        posted_date,
        scheduled_time
    ) auto refresh yes AS
SELECT
    -- CashPayment
    bcp.id,
    bcp.posted_date,
    bcp.received_date,
    -- TODO
    bcp.created_by_id,
    bcp.notes,
    bcp.amount,
    bcp.trace_number,
    bcp.payment_method,
    bcp.doctor_id,
    -- Patient
    bcp.patient_id,
    cp.first_name,
    cp.middle_name,
    cp.last_name,
    -- Office
    cp.chart_id,
    -- Doctor
    '' AS NAME,
    cd.firstname,
    cd.lastname,
    cd.salutation,
    cd.suffix,
    -- Appointment
    cd.practice_group_id,
    -- exam_room: ID and NAME
    -1 AS appointment_id,
    0 AS exam_room_id,
    -- office & appt date
    '' AS exam_room_name,
    -1 AS office_id,
    DATE('1000-01-02') AS service_date_start_date,
    DATE('1000-01-01') AS service_date_end_date,
    '1000-01-02 01:01:01' :: TIMESTAMP AS first_billed_date,
    '1000-01-01 01:01:01' :: TIMESTAMP AS scheduled_time,
    0 AS institutional_claim_flag,
    bli.code
FROM
    chronometer_production.billing_cashpayment bcp
    JOIN chronometer_production.chronometer_doctor cd
    ON (
        bcp.doctor_id = cd.id
    )
    JOIN chronometer_production.chronometer_patient cp
    ON (
        bcp.patient_id = cp.id
    )
    JOIN chronometer_production.billing_billinglineitem bli
    ON (
        bcp.line_item_id = bli.id
    )
WHERE
    (
        cp.is_demo_data_patient IS FALSE
        AND amount <> 0
        AND bcp.appointment_id IS NULL
    );
-- Unallocated
    CREATE materialized VIEW PUBLIC.daysheet_patientpayments_unallocated_not_bli_mv sortkey(
        doctor_id,
        posted_date,
        scheduled_time
    ) auto refresh yes AS
SELECT
    -- CashPayment
    bcp.id,
    bcp.posted_date,
    bcp.received_date,
    -- TODO
    bcp.created_by_id,
    bcp.notes,
    bcp.amount,
    bcp.trace_number,
    bcp.payment_method,
    bcp.doctor_id,
    -- Patient
    bcp.patient_id,
    cp.first_name,
    cp.middle_name,
    cp.last_name,
    -- Office
    cp.chart_id,
    -- Doctor
    '' AS NAME,
    cd.firstname,
    cd.lastname,
    cd.salutation,
    cd.suffix,
    -- Appointment
    cd.practice_group_id,
    -- exam_room: ID and NAME
    -1 AS appointment_id,
    0 AS exam_room_id,
    -- office & appt date
    '' AS exam_room_name,
    -1 AS office_id,
    DATE('1000-01-02') AS service_date_start_date,
    DATE('1000-01-01') AS service_date_end_date,
    '1000-01-02 01:01:01' :: TIMESTAMP AS first_billed_date,
    '1000-01-01 01:01:01' :: TIMESTAMP AS scheduled_time,
    0 AS institutional_claim_flag,
    bcp.line_item_id AS code
FROM
    chronometer_production.billing_cashpayment bcp
    JOIN chronometer_production.chronometer_doctor cd
    ON (
        bcp.doctor_id = cd.id
    )
    JOIN chronometer_production.chronometer_patient cp
    ON (
        bcp.patient_id = cp.id
    )
WHERE
    (
        cp.is_demo_data_patient IS FALSE
        AND amount <> 0
        AND bcp.appointment_id IS NULL
        AND bcp.line_item_id IS NULL
    );
-- merge them together
