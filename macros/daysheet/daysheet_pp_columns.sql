{%- macro daysheet_pp_columns(allocated, bli) -%}
{#/*
    allocated (True, False): whether to join on bcp.appointment_id
    bli (True, False): whether to join on bcp.line_item_id
*/#}
{%- set bcp_fields = [
                     "id",
                     "posted_date",
                     "received_date",
                     "created_by_id",
                     "notes",
                     "amount",
                     "trace_number",
                     "payment_method",
                     "doctor_id",
                     "patient_id",
                     "parent_id"
                     ]
%}
{%- set appt_fields = [
                       "office_id",
                       "service_date_start_date",
                       "service_date_end_date",
                       "first_billed_date",
                       "scheduled_time",
                       "institutional_claim_flag"
                       ]
-%}
{%- set patient_fields = [
    "first_name", "middle_name", "last_name", "chart_id"
    ]
%}
{%- set doctor_fields = [
    "firstname", "lastname", "salutation", "suffix", "practice_group_id"
    ]
-%}
    -- CashPayment
    {% for bcp_f in bcp_fields -%}
        bcp.{{bcp_f}},
    {% endfor -%}
    -- Patient
    {% for pt_f in patient_fields -%}
        cp.{{pt_f}},
    {% endfor -%}
    {%- if allocated -%}
        co.name,
    {%- else -%}
        '' AS name,
    {%- endif %}
    -- Doctor
    {% for doc_f in doctor_fields -%}
        cd.{{doc_f}},
    {% endfor -%}
    -- Appointment
    {#- /*
        IF statement:
        For CashPayments with associated Appointments
        */
    -#}
    {%- if allocated %}
    bcp.appointment_id,
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
    {% for appt_f in appt_fields -%}
        ca.{{appt_f}},
    {% endfor %}
    {%- else %}
    -1 AS appointment_id,
    0  AS exam_room_id,
    '' AS exam_room_name,
    -1 AS office_id,
    date('1000-01-02')               AS service_date_start_date,
    date('1000-01-01')               AS service_date_end_date,
    '1000-01-02 01:01:01'::timestamp AS first_billed_date,
    '1000-01-01 01:01:01'::timestamp AS scheduled_time,
    0                                AS institutional_claim_flag,
    {% endif -%}
    {#/* some CashPayments don't have a BLI */-#}
    {%- if bli -%}
    bli.code
    {%- else -%}
    NULL::varchar as code
    {%- endif -%}
{%- endmacro -%}
