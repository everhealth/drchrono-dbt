{%- macro exam_room_name() -%}
    DECODE(
            ca.examination_room,
            1,  co.exam_room_1_name,
            2,  co.exam_room_2_name,
            3,  co.exam_room_3_name,
            4,  co.exam_room_4_name,
            5,  co.exam_room_5_name,
            6,  co.exam_room_6_name,
            7,  co.exam_room_7_name,
            8,  co.exam_room_8_name,
            9,  co.exam_room_9_name,
            10, co.exam_room_10_name,
            11, co.exam_room_11_name,
            12, co.exam_room_12_name,
            13, co.exam_room_13_name,
            14, co.exam_room_14_name,
            15, co.exam_room_15_name,
            ''
    ) AS exam_room_name
{%- endmacro -%}