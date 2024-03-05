{{
    config(
        SORT=[
            "bli_created_at",
            "date_of_service",
            "practice_group_id",
            "doctor_id",
        ]
    )
}}

WITH
    line_items AS (SELECT * FROM {{ ref("stg_line_items") }})

    , appointments AS (SELECT * FROM {{ ref("stg_appointments") }})

    , doctors AS (SELECT * FROM {{ ref("stg_doctors") }})

    , offices AS (SELECT * FROM {{ ref("stg_offices") }})

    , patients AS (SELECT * FROM {{ ref("stg_patients") }})

    , cash_payments AS (SELECT * FROM {{ ref("stg_cash_payments") }})

    , final AS (
        SELECT

            a.*
            , li.*
            , d.*
            , o.*
            , p.*
            , cp.*
            , {{ exam_room_name("a","o") }}
            , CASE -- If icd_version_number is 9, use ICD9 codes
                -- If icd_version_number is 10, use ICD10 codes
                WHEN a.icd_version_number = 9 THEN '["' || REPLACE(a.icd9_diagnosis, ',', '","') || '"]'
                -- If icd_version_number is neither 9 nor 10
                WHEN a.icd_version_number = 10 THEN a.icd10_diagnosis_json
                WHEN
                    a.icd9_diagnosis IS NOT NULL
                    -- Otherwise, default to ICD10
                    AND a.icd9_diagnosis != '' THEN '["' || REPLACE(a.icd9_diagnosis, ',', '","') || '"]'
                ELSE a.icd10_diagnosis_json
            END AS icd_codes_str

            , CASE
                WHEN
                    a.ins1_status != '' AND a.ins2_status = ''
                    THEN p.primary_insurance_plan_type
                WHEN
                    a.appt_billing_status = 'Bill Secondary Insurance'
                    OR (
                        a.appt_billing_status = 'Bill Insurance'
                        AND a.ins1_status = 'Coordination of Benefits'
                    ) THEN p.secondary_insurance_plan_type
                ELSE p.primary_insurance_plan_type
            END AS insurance_plan
            , CASE
                WHEN ins1_status != '' AND ins2_status = '' THEN p.primary_insurance_company
                WHEN
                    appt_billing_status = 'Bill Secondary Insurance'
                    OR (
                        appt_billing_status = 'Bill Insurance'
                        AND ins1_status = 'Coordination of Benefits'
                    ) THEN secondary_insurer_company
                ELSE p.primary_insurance_company
            END AS patient_insurance_company



        FROM line_items AS li
            LEFT JOIN
                appointments AS a
                ON li.li_appointment_id = a.appointment_id AND {{ days_ago("a.appt_updated_at", 90) }}
            LEFT JOIN doctors AS d ON a.appt_doctor_id = d.doctor_id AND {{ filter_pg("d") }}
            LEFT JOIN offices AS o ON a.appt_office_id = o.office_id
            LEFT JOIN patients AS p ON a.appt_patient_id = p.patient_id
            LEFT JOIN cash_payments AS cp ON li.line_item_id = cp.cash_line_item_id
        WHERE li.li_appointment_id IS NULL OR a.appointment_id IS NOT NULL
    )

SELECT * FROM final
