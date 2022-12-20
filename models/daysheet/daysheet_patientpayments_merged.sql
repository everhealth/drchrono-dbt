{{ config(
    tags=["DaySheet"]
) }}
--CREATE VIEW PUBLIC.daysheet_patientpayments_merged AS
SELECT
    derived_table1.id,
    derived_table1.posted_date,
    derived_table1.received_date,
    derived_table1.created_by_id,
    derived_table1.notes,
    derived_table1.amount,
    derived_table1.trace_number,
    derived_table1.payment_method,
    derived_table1.doctor_id,
    derived_table1.patient_id,
    derived_table1.parent_id,
    derived_table1.first_name,
    derived_table1.middle_name,
    derived_table1.last_name,
    derived_table1.chart_id,
    derived_table1.name,
    derived_table1.firstname,
    derived_table1.lastname,
    derived_table1.salutation,
    derived_table1.suffix,
    derived_table1.practice_group_id,
    -- set to NULL if values match those set in ds_pp_unallocated
    NVL2(
        CASE
            WHEN (
                derived_table1.appointment_id < 0
            ) = TRUE THEN NULL :: BOOLEAN
            ELSE derived_table1.appointment_id < 0
        END,
        derived_table1.appointment_id,
        NULL :: INTEGER
    ) AS appointment_id,
    exam_room_id,
    exam_room_name,
    NVL2(
        CASE
            WHEN (
                derived_table1.office_id < 0
            ) = TRUE THEN NULL :: BOOLEAN
            ELSE derived_table1.office_id < 0
        END,
        derived_table1.office_id,
        NULL :: INTEGER
    ) AS office_id,
    NVL2(
        CASE
            WHEN (
                derived_table1.service_date_start_date < '1200-01-01' :: DATE
            ) = TRUE THEN NULL :: BOOLEAN
            ELSE derived_table1.service_date_start_date < '1200-01-01' :: DATE
        END,
        derived_table1.service_date_start_date,
        NULL :: DATE
    ) AS service_date_start_date,
    NVL2(
        CASE
            WHEN (
                derived_table1.service_date_end_date < '1200-01-01' :: DATE
            ) = TRUE THEN NULL :: BOOLEAN
            ELSE derived_table1.service_date_end_date < '1200-01-01' :: DATE
        END,
        derived_table1.service_date_end_date,
        NULL :: DATE
    ) AS service_date_end_date,
    NVL2(
        CASE
            WHEN (
                derived_table1.first_billed_date < '1200-01-01 00:00:00' :: TIMESTAMP without TIME ZONE
            ) = TRUE THEN NULL :: BOOLEAN
            ELSE derived_table1.first_billed_date < '1200-01-01 00:00:00' :: TIMESTAMP without TIME ZONE
        END,
        derived_table1.first_billed_date,
        NULL :: TIMESTAMP without TIME ZONE
    ) AS first_billed_date,
    NVL2(
        CASE
            WHEN (
                derived_table1.scheduled_time < '1200-01-01 00:00:00' :: TIMESTAMP without TIME ZONE
            ) = TRUE THEN NULL :: BOOLEAN
            ELSE derived_table1.scheduled_time < '1200-01-01 00:00:00' :: TIMESTAMP without TIME ZONE
        END,
        derived_table1.scheduled_time,
        NULL :: TIMESTAMP without TIME ZONE
    ) AS scheduled_time,
    derived_table1.institutional_claim_flag,
    derived_table1.code
FROM
    (
        SELECT
            *
        FROM
            {{ref('daysheet_patientpayments_allocated_bli_mv')}}
        UNION ALL
        SELECT
            *
        FROM
            {{ref('daysheet_patientpayments_allocated_not_bli_mv')}}
        UNION ALL
        SELECT
            *
        FROM
            {{ref('daysheet_patientpayments_unallocated_bli_mv')}}
        UNION ALL
        SELECT
            *
        FROM
            {{ref('daysheet_patientpayments_unallocated_not_bli_mv')}}
    ) derived_table1;
