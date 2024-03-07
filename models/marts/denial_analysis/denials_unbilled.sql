{{ 
    config(
        SORT=["practice_group_id", "doctor_id"],
        materialized="table"
    )
}}
SELECT
    lit.id
    , lit.code
    , lit.adjustment_reason
    , lit.created_at
    , lit.posted_date
    , CASE
        WHEN lit.adjusted_adjustment_reason IN ('ADJUST_INS', 'ADJUST_PT') THEN lit.adjustment
        ELSE 0
    END             AS zeroed_adjustment
    , lit.adjustment
    , ca.id         AS appointment_id
    , ca.billing_provider_id
    , ca.primary_insurer_payer_id
    , ca.primary_insurer_company
    , ca.secondary_insurer_payer_id
    , ca.secondary_insurer_company
    , ca.icd_version_number
    , ca.icd9_diagnosis
    , ca.icd10_diagnosis_json
    , CASE
        WHEN
            NOT lit.appointment_id
            OR ins_idx = 0 THEN 'N/A'
        WHEN lit.ins_idx = 1
            THEN COALESCE(
                ca.primary_insurer_company || ' (' || ca.primary_insurer_payer_id || ')'
                , ca.primary_insurer_company
                , ca.primary_insurer_payer_id
            )
        WHEN lit.ins_idx = 2
            THEN COALESCE(
                ca.secondary_insurer_company || ' (' || ca.secondary_insurer_payer_id || ')'
                , ca.secondary_insurer_company
                , ca.secondary_insurer_payer_id
            )
        ELSE 'N/A'
    END             AS insurance_name
    , ca.scheduled_time
    , ca.office_id
    , ca.examination_room
    , ca.billing_status
    , ca.ins1_status
    , ca.ins2_status
    , bli.denied_flag
    , bli.balance_pt
    , bli.balance_ins
    , bli.code      AS bli_code
    , cp.first_name
    , cp.middle_name
    , cp.last_name
    , CASE
        WHEN
            cd.salutation IS NOT NULL
            AND cd.suffix IS NOT NULL
            THEN cd.salutation || ' ' || cd.firstname || ' ' || cd.lastname || ', ' || cd.suffix
        WHEN cd.salutation IS NOT NULL THEN cd.salutation || ' ' || cd.firstname || cd.lastname
        WHEN cd.suffix IS NOT NULL THEN cd.firstname || ' ' || cd.lastname || ', ' || cd.suffix
        ELSE cd.firstname || ' ' || cd.lastname
    END             AS doctor_name
    , cd.firstname  AS doc_firstname
    , cd.lastname   AS doc_lastname
    , cd.suffix     AS doc_suffix
    , cd.salutation AS doc_salutation
    , cd.id         AS doctor_id
    , cd.practice_group_id
FROM
    {{ source("chronometer_production","billing_lineitemtransaction") }} AS lit
    /*
             LEFT OUTER JOIN billing_eraobject ON (billing_lineitemtransaction.era_id = billing_eraobject.id)
             INNER JOIN billing_billinglineitem ON (billing_lineitemtransaction.line_item_id = billing_billinglineitem.id)
             INNER JOIN chronometer_appointment ON (billing_lineitemtransaction.appointment_id = chronometer_appointment.id)
             LEFT OUTER JOIN chronometer_patient ON (billing_lineitemtransaction.patient_id = chronometer_patient.id)
             */
    LEFT OUTER JOIN {{ source("chronometer_production", "billing_eraobject") }} AS era ON (lit.era_id = era.id)
    INNER JOIN {{ source("chronometer_production", "billing_billinglineitem") }} AS bli ON (lit.line_item_id = bli.id)
    INNER JOIN {{ source("chronometer_production", "chronometer_appointment") }} AS ca ON (lit.appointment_id = ca.id)
    INNER JOIN {{ source("chronometer_production", "chronometer_patient") }} AS cp ON (ca.patient_id = cp.id)
    INNER JOIN {{ source("chronometer_production", "chronometer_doctor") }} AS cd ON (lit.doctor_id = cd.id)
WHERE
    denied_flag = TRUE
    AND (
        (
            ins1_status = 'ERA Denied'
            AND ca.billing_status != 'Bill Secondary Insurance'
        )
        OR ins2_status = 'ERA Denied'
    )
    AND lit.adjustment_reason IN (
        '10'
        , '101'
        , '102'
        , '106'
        , '107'
        , '108'
        , '109'
        , '11'
        , '110'
        , '111'
        , '112'
        , '114'
        , '116'
        , '117'
        , '119'
        , '12'
        , '121'
        , '125'
        , '129'
        , '13'
        , '132'
        , '133'
        , '134'
        , '135'
        , '136'
        , '138'
        , '139'
        , '14'
        , '140'
        , '143'
        , '146'
        , '147'
        , '148'
        , '149'
        , '15'
        , '150'
        , '151'
        , '152'
        , '153'
        , '154'
        , '157'
        , '158'
        , '159'
        , '16'
        , '162'
        , '163'
        , '164'
        , '165'
        , '166'
        , '167'
        , '168'
        , '169'
        , '170'
        , '171'
        , '172'
        , '173'
        , '174'
        , '175'
        , '176'
        , '177'
        , '179'
        , '18'
        , '180'
        , '181'
        , '182'
        , '183'
        , '184'
        , '185'
        , '186'
        , '188'
        , '189'
        , '19'
        , '190'
        , '191'
        , '192'
        , '193'
        , '194'
        , '195'
        , '197'
        , '198'
        , '199'
        , '20'
        , '200'
        , '202'
        , '203'
        , '204'
        , '206'
        , '207'
        , '208'
        , '209'
        , '21'
        , '210'
        , '211'
        , '212'
        , '213'
        , '214'
        , '215'
        , '216'
        , '217'
        , '218'
        , '219'
        , '22'
        , '220'
        , '221'
        , '222'
        , '224'
        , '226'
        , '227'
        , '228'
        , '229'
        , '23'
        , '230'
        , '231'
        , '232'
        , '233'
        , '234'
        , '235'
        , '236'
        , '24'
        , '242'
        , '243'
        , '251'
        , '252'
        , '254'
        , '256'
        , '257'
        , '258'
        , '259'
        , '26'
        , '260'
        , '261'
        , '267'
        , '268'
        , '269'
        , '27'
        , '270'
        , '271'
        , '272'
        , '273'
        , '274'
        , '275'
        , '276'
        , '277'
        , '288'
        , '29'
        , '31'
        , '32'
        , '33'
        , '34'
        , '39'
        , '4'
        , '40'
        , '49'
        , '5'
        , '50'
        , '51'
        , '53'
        , '54'
        , '55'
        , '56'
        , '58'
        , '59'
        , '6'
        , '60'
        , '61'
        , '69'
        , '7'
        , '70'
        , '74'
        , '75'
        , '76'
        , '78'
        , '8'
        , '85'
        , '87'
        , '89'
        , '9'
        , '90'
        , '91'
        , '94'
        , '95'
        , '96'
        , '97'
        , 'A0'
        , 'A1'
        , 'A5'
        , 'A6'
        , 'A7'
        , 'A8'
        , 'B1'
        , 'B10'
        , 'B11'
        , 'B12'
        , 'B13'
        , 'B14'
        , 'B15'
        , 'B16'
        , 'B20'
        , 'B22'
        , 'B23'
        , 'B4'
        , 'B5'
        , 'B7'
        , 'B8'
        , 'B9'
        , 'P1'
        , 'P10'
        , 'P11'
        , 'P13'
        , 'P14'
        , 'P16'
        , 'P17'
        , 'P18'
        , 'P19'
        , 'P2'
        , 'P20'
        , 'P21'
        , 'P22'
        , 'P23'
        , 'P4'
        , 'P5'
        , 'P6'
        , 'P7'
        , 'P8'
        , 'P9'
        , 'W1'
    )
